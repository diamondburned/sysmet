package sysmet

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"git.unix.lgbt/diamondburned/sysmet/v3/internal/badgerlog"
	"github.com/dgraph-io/badger/v3"
	"github.com/fxamacker/cbor/v2"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"

	badgeropts "github.com/dgraph-io/badger/v3/options"
)

// Bucket names containing sysmet versions.
//
// We no longer have buckets with badger, so we use a minimal prefix instead,
// which we should keep as short as possible.
var (
	bucketName   = []byte("s3")
	bucketNameV2 = []byte("sysmet-v2")
	bucketNameV1 = []byte("sysmet-v1")
)

var (
	bPoints = []byte("p")
)

// Version is the type for the version of the Sysmet database format.
type Version uint8

const (
	Version1 Version = iota
	Version2
	Version3
)

// CurrentVersion is the version that Sysmet snapshots will be written as.
const CurrentVersion = Version3

// this is never a valid JSON character
const versionBytePrefix = 0xFE

// Convenient inaccurate time constants.
const (
	Day   = 24 * time.Hour
	Week  = 7 * Day
	Month = 30 * Day
)

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
	db *badger.DB
	ro bool
}

// Open opens a database. Databases must be closed once they're done.
func Open(path string, write bool) (*Database, error) {
	var v2db *LegacyDatabase

	// Check if path is a V2 database, but we only care if write is true. That
	// way, we can do migrations right here.
	if write {
		var err error

		// Immediately try and open the V2 database. Badger should immediately
		// call OpenFile with O_EXCL before doing anything else, so it's not
		// super expensive to do this.
		v2db, err = OpenLegacyExisting(path)
		if err == nil {
			defer v2db.Close()

			// We acquired the database lock. We'll remove it to a new path, so
			// we can open our v3 database without any issues.
			if err := os.Rename(path, path+".legacy"); err != nil {
				v2db.Close()
				return nil, errors.Wrap(err, "failed to rename v2 database")
			}
		}
	}

	opts := badger.LSMOnlyOptions(path)
	opts.InMemory = path == ":memory:"
	opts.ReadOnly = !write
	opts.Compression = badgeropts.ZSTD
	opts.ZSTDCompressionLevel = 1
	opts.DetectConflicts = false
	opts.MetricsEnabled = false
	opts.NumGoroutines = 4
	opts.NumCompactors = 4
	opts.Logger = badgerlog.NewDefaultLogger()

	b, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Wrap(err, "bbolt")
	}

	v3db := &Database{
		db: b,
		ro: !write,
	}

	// If we're writing and we have a V2 database previously, then we can
	// migrate the database.
	if v2db != nil {
		if err := v2db.MigrateV2toV3(v3db); err != nil {
			v3db.Close()
			return nil, errors.Wrap(err, "failed to migrate v2 to v3")
		}
	}

	return v3db, nil
}

// Close closes the database. Calling Close twice does nothing.
func (db *Database) Close() error {
	return db.db.Close()
}

// GC cleans up the database. Since this is a fairly expensive operation, it
// should only be called rarely.
func (db *Database) GC(age time.Duration) error {
	if db.ro {
		return errors.New("database not writable")
	}

	now := convertWithUnixZero(time.Now())
	sec := uint32(age / time.Second)

	return db.gc(now, sec)
}

func (db *Database) gc(now, sec uint32) error {
	// precalculate the key for seeking.
	before := unixToBE(now - sec)

	return db.db.Update(func(tx *badger.Txn) error {
		iter := tx.NewKeyIterator(bkey(bPoints), badger.IteratorOptions{
			Reverse: true,
		})
		iter.Rewind()
		defer iter.Close()

		for iter.Seek(bkey(bPoints, before)); iter.Valid(); iter.Next() {
			item := iter.Item()
			if err := tx.Delete(item.KeyCopy(nil)); err != nil {
				return errors.Wrap(err, "failed to delete under iterator")
			}
		}

		return nil
	})
}

// Update updates a set of prepared metrics into the database.
func (db *Database) Update(snapshot Snapshot) error {
	if db.ro {
		return errors.New("database not writable")
	}

	k := unixToBE(snapshot.time)
	v, err := encodeSnapshot(snapshot)
	if err != nil {
		return err
	}

	if err = db.db.Update(func(tx *badger.Txn) error {
		return tx.Set(bkey(bPoints, k), v)
	}); err != nil {
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
	buf.WriteByte(byte(CurrentVersion))

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
	case Version2, Version3:
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

// keysep is a separator used to separate keys in the database.
var keysep = []byte{0x00}

func bkeyTrim(b []byte, prefixes ...[]byte) []byte {
	b = bytes.TrimPrefix(b, bucketName)
	b = bytes.TrimPrefix(b, keysep)
	for _, prefix := range prefixes {
		b = bytes.TrimPrefix(b, prefix)
		b = bytes.TrimPrefix(b, keysep)
	}
	return b
}

func bkey(parts ...[]byte) []byte {
	var buf bytes.Buffer
	buf.Grow(16)
	buf.Write(bucketName)
	for _, part := range parts {
		buf.Write(keysep)
		buf.Write(part)
	}
	return buf.Bytes()
}

// Iterator returns a new database iterator with second precision. The iterator
// must be closed after it's done.
func (db *Database) Iterator(opts IteratorOpts) (*Iterator, error) {
	return newIterator(db.db, opts)
}
