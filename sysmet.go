package sysmet

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"time"

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
var bucketName = []byte("sysmet-v1")

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

	Time uint32 `json:"-"`
}

// PrepareMetrics prepares a set of metrics to write at once.
func PrepareMetrics() (Snapshot, error) {
	var err error

	snapshot := Snapshot{
		Time: uint32(time.Now().Unix()),
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

	return db.db.Batch(func(tx *bbolt.Tx) error {
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

// Update updates a set of prepared metrics into the database.
func (db *Database) Update(snapshot Snapshot) error {
	if db.db.IsReadOnly() {
		return errors.New("database not writable")
	}

	v, err := json.Marshal(snapshot)
	if err != nil {
		return errors.Wrap(err, "failed to marshal")
	}

	k := unixToBE(snapshot.Time)

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
