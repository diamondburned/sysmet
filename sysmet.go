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

// Snapshot describes a single snapshot of data written. It implements
// sort.Interface.
type Snapshot struct {
	CPUs      []cpu.TimesStat
	Memory    *mem.VirtualMemoryStat
	Network   []net.IOCountersStat
	Disks     []disk.UsageStat
	Temps     []host.TemperatureStat
	LoadAvgs  *load.AvgStat
	HostStats *load.MiscStat

	Time uint32 `json:"-"`
}

// zeroSnapshot is a zero-value snapshot containing no nil pointers.
var zeroSnapshot = Snapshot{
	CPUs:      []cpu.TimesStat{},
	Memory:    &mem.VirtualMemoryStat{},
	Network:   []net.IOCountersStat{},
	Disks:     []disk.UsageStat{},
	Temps:     []host.TemperatureStat{},
	LoadAvgs:  &load.AvgStat{},
	HostStats: &load.MiscStat{},
}

// PrepareMetrics prepares a set of metrics to write at once.
func PrepareMetrics() (Snapshot, error) {
	var err error

	snapshot := Snapshot{
		Time: uint32(time.Now().Unix()),
	}

	metricUpdates := map[string]func(){
		"cpu":  func() { snapshot.CPUs, err = cpu.Times(true) },
		"mem":  func() { snapshot.Memory, err = mem.VirtualMemory() },
		"net":  func() { snapshot.Network, err = net.IOCounters(true) },
		"disk": func() { snapshot.Disks, err = diskUsages() },
		"temp": func() { snapshot.Temps, err = host.SensorsTemperatures() },
		"load": func() { snapshot.LoadAvgs, err = load.Avg() },
		"host": func() { snapshot.HostStats, err = load.Misc() },
	}

	for key, fn := range metricUpdates {
		fn()
		if err != nil {
			return snapshot, errors.Wrapf(err, "failed to get %s", key)
		}
	}

	return snapshot, nil
}

func diskUsages() ([]disk.UsageStat, error) {
	pstat, err := disk.Partitions(false)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get all partitions")
	}

	ustat := make([]disk.UsageStat, 0, len(pstat))
	for _, p := range pstat {
		u, err := disk.Usage(p.Mountpoint)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get usage for mount %q", p.Mountpoint)
		}
		ustat = append(ustat, *u)
	}

	return ustat, nil
}

// TTL is the time-to-live for all keys.
var TTL = 365 * 24 * time.Hour

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

// ReadOpts is the options for reading. It describes the range of data to read.
type ReadOpts struct {
	// Start is the time to stop reading the metrics backwards. By default, the
	// zero-value is used, which would read all metrics. The Start time must
	// ALWAYS be before End.
	Start time.Time
	// End is the time to start reading the metrics backwards. The default
	// zero-value means to read from the latest point.
	End time.Time
	// Precision is the gap between each metrics point, which is minimum a
	// second, in multiples of seconds. It only works if the write frequency is
	// more than the precision.
	Precision time.Duration
}

// Read returns a new database reader/iterator with second precision. The reader
// must be closed after it's done.
func (db *Database) Read(opts ReadOpts) (*Reader, error) {
	if !opts.Start.IsZero() && !opts.End.IsZero() {
		if !opts.End.After(opts.Start) {
			return nil, errors.New("opts.End should be after opts.Start")
		}
	}

	// Round the precision to be in multiples of seconds.
	opts.Precision = opts.Precision.Round(time.Second)
	// Enforce the minimum second precision.
	if opts.Precision < time.Second {
		opts.Precision = time.Second
	}

	start := convertWithUnixZero(opts.Start)
	end := convertWithUnixZero(opts.End)

	r := Reader{
		start: start,
		end:   end,
		prev:  end,
		prec:  uint32(opts.Precision / time.Second),
	}

	tx, err := db.db.Begin(false)
	if err != nil {
		return nil, errors.Wrap(err, "failed to begin tx")
	}

	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		return nil, ErrUninitialized
	}

	cursor := bucket.Cursor()

	if r.end > 0 {
		seek := unixToBE(r.end)
		r.key, r.value = cursor.Seek(seek)
	} else {
		r.key, r.value = cursor.Last()
	}

	r.tx = tx
	r.bk = bucket
	r.cs = cursor

	return &r, nil
}

// Reader is a backwards metric reader that allows aggregation over metrics.
//
// The reader supplies a set of methods that take in a closure callback to be
// called on each data point. If the callback returns an error, then the
// iteration is halted and the error is returned up. If the error is StopRead,
// then the iteration is stopped and no errors are returned.
type Reader struct {
	tx *bbolt.Tx
	bk *bbolt.Bucket
	cs *bbolt.Cursor

	// current state
	key   []byte
	value []byte
	prev  uint32

	// constants
	start uint32
	end   uint32
	prec  uint32
}

// Close closes the reader.
func (r *Reader) Close() error {
	return r.tx.Rollback()
}

// ReadAll reads all of the reader from start to end. The reader is closed when
// this function is returned. This function will return points non-existent in
// the database if both start and end were given.
func (r *Reader) ReadAll() []Snapshot {
	var snapshots []Snapshot
	var snapshot Snapshot

	for r.Prev(&snapshot) {
		snapshots = append(snapshots, snapshot)
		snapshot = Snapshot{}
	}

	r.Close()

	// https://github.com/golang/go/wiki/SliceTricks
	// Reverse the snapshots so that the latest entries are first.
	for i := len(snapshots)/2 - 1; i >= 0; i-- {
		opposite := len(snapshots) - 1 - i
		snapshots[i], snapshots[opposite] = snapshots[opposite], snapshots[i]
	}

	return snapshots
}

// ReadExact reads exactly from start to end; the function returns nil if start
// or end is 0. The reader is closed when this function is returned. This
// function will return a slice with non-existent points as padding.
func (r *Reader) ReadExact() []Snapshot {
	if r.start == 0 || r.end == 0 {
		panic("r.start or r.end is zero")
	}

	last := (r.end - r.start) / r.prec

	snapshots := make([]Snapshot, last)

	for last > 0 && r.Prev(&snapshots[last-1]) {
		last--
	}

	r.Close()

	// Pad unread snapshots.
	for i, snapshot := range snapshots {
		if snapshot.Time == 0 {
			snapshots[i] = zeroSnapshot
		}
	}

	return snapshots
}

// step updates the internal cursor backwards.
func (r *Reader) step() {
	r.key, r.value = r.cs.Prev()
}

// isValid returns true if the reader is still valid.
func (r *Reader) isValid() bool { return r.key != nil }

// Prev reads the previous item into the given snapshot pointer or the last item
// if the Reader has never been used before. False is returned if nothing is
// read and the reader is closed, otherwise true is.
func (r *Reader) Prev(snapshot *Snapshot) bool {
	for r.isValid() {
		time := readUnixBE(r.key)

		if r.start > 0 && r.start > time {
			break
		}

		// Skip if the current item is within the current frame.
		if r.prev > 0 && time > r.prev {
			r.step()
			continue
		}

		// Unmarshal fail is a fatal error, so we invalidate everything.
		if err := json.Unmarshal(r.value, snapshot); err != nil {
			break
		}

		// Update the timestamp.
		snapshot.Time = time

		// Get the next tick from the current time, whichever is further.
		r.prev -= r.prec
		if prev := time - r.prec; prev < r.prev {
			r.prev = prev
		}

		// Seek for the next call.
		r.step()

		return true
	}

	r.Close()
	return false
}
