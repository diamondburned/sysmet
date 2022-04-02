package sysmet

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
	"go.etcd.io/bbolt"
)

// Bucket names containing sysmet versions.
var (
	bucketName   = []byte("sysmet-v2")
	bucketNameV1 = []byte("sysmet-v1")
)

// Version is the type for the version of the Sysmet database format.
type Version uint8

const (
	Version1 Version = iota
	Version2
)

// this is never a valid JSON character
const versionBytePrefix = 0xFE

// Convenient inaccurate time constants.
const (
	Day   = 24 * time.Hour
	Week  = 7 * Day
	Month = 30 * Day
)

// CurrentVersion is the version that Sysmet snapshots will be written as.
const CurrentVersion = Version2

// ErrUninitialized is returned when sysmet's bucket is not found in the
// database. This may happen when the database has never been updated before.
var ErrUninitialized = errors.New("bucket not initialized")

// Snapshot describes a single snapshot of data.
type Snapshot struct {
	CPUs      []cpu.TimesStat
	Memory    mem.VirtualMemoryStat
	Swap      mem.SwapMemoryStat
	Network   []net.IOCountersStat
	Disks     []disk.UsageStat
	Temps     []host.TemperatureStat
	LoadAvgs  load.AvgStat
	HostStats load.MiscStat

	time uint32
}

// PrepareMetrics prepares a set of metrics to write at once.
func PrepareMetrics() (Snapshot, error) {
	var err error

	snapshot := Snapshot{
		time: uint32(time.Now().Unix()),
	}

	metricUpdates := map[string]func(){
		"cpu":  func() { snapshot.CPUs, err = cpu.Times(true) },
		"net":  func() { snapshot.Network, err = net.IOCounters(true) },
		"disk": func() { snapshot.Disks, err = diskUsages() },
		"temp": func() { snapshot.Temps, err = host.SensorsTemperatures() },
		// For these functions, see ps.go.
		"mem":  func() { snapshot.Memory, err = virtualMemory() },
		"swap": func() { snapshot.Swap, err = swapMemory() },
		"load": func() { snapshot.LoadAvgs, err = loadAvg() },
		"host": func() { snapshot.HostStats, err = loadMisc() },
	}

	for key, fn := range metricUpdates {
		fn()
		if err != nil {
			return snapshot, errors.Wrapf(err, "failed to get %s", key)
		}
	}

	return snapshot, nil
}

func (s Snapshot) Time() time.Time { return time.Unix(s.UnixTime(), 0) }
func (s Snapshot) UnixTime() int64 { return int64(s.time) }

// Database describes a wrapped database instance.
type Database struct {
	db *bbolt.DB
}

// Open opens a database. Databases must be closed once they're done.
func Open(path string, write bool) (*Database, error) {
	b, err := bbolt.Open(path, os.ModePerm, &bbolt.Options{
		Timeout:      time.Minute,
		FreelistType: bbolt.FreelistArrayType,
		ReadOnly:     !write,
	})
	if err != nil {
		return nil, errors.Wrap(err, "bbolt")
	}

	return &Database{b}, nil
}

// Close closes the database. Calling Close twice does nothing.
func (db *Database) Close() error {
	return db.db.Close()
}

// GC cleans up the database. Since this is a fairly expensive operation, it
// should only be called rarely.
func (db *Database) GC(age time.Duration) error {
	if db.db.IsReadOnly() {
		return errors.New("database not writable")
	}

	now := convertWithUnixZero(time.Now())
	sec := uint32(age / time.Second)

	return db.gc(now, sec)
}

func (db *Database) gc(now, sec uint32) error {
	// precalculate the key for seeking.
	before := unixToBE(now - sec)

	return db.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return ErrUninitialized
		}

		cs := b.Cursor()

		for k, _ := cs.Seek(before); k != nil; k, _ = cs.Prev() {
			if err := cs.Delete(); err != nil {
				return errors.Wrap(err, "failed to delete under cursor")
			}
		}

		return nil
	})
}

// MigrateFromV1 migrates the database from v1.
func (db *Database) MigrateFromV1(max time.Duration) error {
	if db.db.IsReadOnly() {
		return errors.New("database not writable")
	}

	now := convertWithUnixZero(time.Now())
	sec := uint32(max / time.Second)

	if err := db.gc(now, sec); err != nil {
		return errors.Wrap(err, "cannot gc database")
	}

	before := unixToBE(now - sec)

	return db.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketNameV1)
		if b == nil {
			return ErrUninitialized
		}

		cs := b.Cursor()
		var s Snapshot
		var buf bytes.Buffer
		buf.Grow(1024)

		for k, v := cs.Seek(before); k != nil; k, v = cs.Next() {
			// Try and delete. Ignore if it's a bucket.
			if err := cs.Delete(); err != nil {
				continue
			}

			s = Snapshot{}
			buf.Reset()

			if err := decodeSnapshot(v, &s); err != nil {
				continue
			}

			// Re-encode.
			if err := encodeSnapshotBuf(s, &buf); err != nil {
				// If we can't reencode a valid snapshot, then that's not good.
				return errors.Wrap(err, "cannot encode snapshot")
			}

			if err := b.Put(k, buf.Bytes()); err != nil {
				return errors.Wrap(err, "cannot put into database")
			}
		}

		return nil
	})
}

// Update updates a set of prepared metrics into the database.
func (db *Database) Update(snapshot Snapshot) error {
	if db.db.IsReadOnly() {
		return errors.New("database not writable")
	}

	v, err := encodeSnapshot(snapshot)
	if err != nil {
		return err
	}
	k := unixToBE(snapshot.time)

	tx := func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return errors.Wrap(err, "failed to create bucket")
		}

		return b.Put(k, v)
	}

	if err = db.db.Update(tx); err != nil {
		return errors.Wrap(err, "failed to update db")
	}

	return nil
}

func encodeSnapshot(snapshot Snapshot) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(8192)
	err := encodeSnapshotBuf(snapshot, &buf)
	return buf.Bytes(), err
}

func encodeSnapshotBuf(snapshot Snapshot, buf *bytes.Buffer) error {
	buf.WriteByte(versionBytePrefix)
	buf.WriteByte(byte(Version2))

	if err := cbor.NewEncoder(buf).Encode(snapshot); err != nil {
		return errors.Wrap(err, "failed to marshal")
	}

	return nil
}

func decodeSnapshot(b []byte, dst *Snapshot) (err error) {
	if len(b) < 2 || b[0] != versionBytePrefix {
		err = json.Unmarshal(b, dst)
		return
	}

	version := Version(b[1])
	b = b[2:]

	switch version {
	case Version1:
		err = json.Unmarshal(b, dst)
	case Version2:
		err = cbor.Unmarshal(b, dst)
	default:
		err = fmt.Errorf("unknown version %q", version)
	}

	return
}

func unixToBE(unix uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b[:], unix)
	return b
}

func readUnixBE(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// convertWithUnixZero converts a time.Time to Unix, or if time.Time is zero,
// then 0 is returned.
func convertWithUnixZero(t time.Time) uint32 {
	if t.IsZero() {
		return 0
	}
	return uint32(t.Unix())
}

// Iterator returns a new database iterator with second precision. The iterator
// must be closed after it's done.
func (db *Database) Iterator(opts IteratorOpts) (*Iterator, error) {
	return newIterator(db.db, opts)
}
