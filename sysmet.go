package sysmet

import (
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

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
	db    *badger.DB
	write bool
}

// Open opens a database. Databases must be closed once they're done.
func Open(path string, write bool) (*Database, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	opts.ReadOnly = !write
	opts.SyncWrites = write

	b, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open db")
	}

	return &Database{b, write}, nil
}

// Close closes the database. Calling Close twice does nothing.
func (db *Database) Close() error {
	return db.db.Close()
}

// GC cleans up the database. Since this is a fairly expensive operation, it
// should only be called rarely.
func (db *Database) GC(age time.Duration) error {
	if !db.write {
		return errors.New("database not writable")
	}

	now := convertWithUnixZero(time.Now())
	sec := uint32(age / time.Second)

	// Collect value garbage before running log GC.
	err := db.gc(now, sec)
	vlgcErr := db.db.RunValueLogGC(0.5)

	if err != nil {
		return err
	}

	return vlgcErr
}

func (db *Database) gc(now, sec uint32) error {
	// precalculate the key for seeking.
	before := unixToBE(now - sec)

	return db.db.Update(func(tx *badger.Txn) error {
		iter := tx.NewIterator(badger.IteratorOptions{
			Reverse: true, // reverse for seeking
		})
		defer iter.Close()

		// Only store the last error.
		var lastErr error

		for iter.Seek(before); iter.Valid(); iter.Next() {
			// Copy the keys into a new byte slice, because Delete will
			// reference it until the end of the txn per documentation.
			key := iter.Item().KeyCopy(nil)

			if err := tx.Delete(key); err != nil {
				lastErr = err
			}
		}

		if lastErr != nil {
			return errors.Wrap(lastErr, "last error encountered while deleting")
		}

		return nil
	})
}

// Update updates a set of prepared metrics into the database.
func (db *Database) Update(snapshot Snapshot) error {
	if !db.write {
		return errors.New("database not writable")
	}

	b, err := json.Marshal(snapshot)
	if err != nil {
		return errors.Wrap(err, "failed to marshal")
	}

	entry := badger.Entry{
		Key:       unixToBE(snapshot.Time),
		Value:     b,
		ExpiresAt: uint64(snapshot.Time) + uint64(TTL/time.Second),
	}

	err = db.db.Update(func(tx *badger.Txn) error { return tx.SetEntry(&entry) })
	if err != nil {
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
func (db *Database) Read(opts ReadOpts) *Reader {
	if !opts.Start.IsZero() && !opts.End.IsZero() {
		if !opts.End.After(opts.Start) {
			panic("end > start assertion failed")
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

	txn := db.db.NewTransaction(false)
	iter := txn.NewIterator(badger.IteratorOptions{Reverse: true})

	if r.end > 0 {
		seek := unixToBE(r.end)
		iter.Seek(seek[:])
	} else {
		iter.Rewind()
	}

	r.txn = txn
	r.iter = iter

	return &r
}

// Reader is a backwards metric reader that allows aggregation over metrics.
//
// The reader supplies a set of methods that take in a closure callback to be
// called on each data point. If the callback returns an error, then the
// iteration is halted and the error is returned up. If the error is StopRead,
// then the iteration is stopped and no errors are returned.
type Reader struct {
	txn  *badger.Txn
	iter *badger.Iterator

	start uint32
	end   uint32
	prec  uint32
	prev  uint32
}

// Close closes the reader.
func (r *Reader) Close() {
	r.iter.Close()
	r.txn.Discard()
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

// ReadExact reads exactly from start to end; the function panics if start or
// end is 0. The reader is closed when this function is returned. This function
// will return a slice with non-existent points as padding.
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

// Prev reads the previous item into the given snapshot pointer or the last item
// if the Reader has never been used before. False is returned if nothing is
// read and the reader is closed, otherwise true is.
func (r *Reader) Prev(snapshot *Snapshot) bool {
	for r.iter.Valid() {
		switch r.read(snapshot) {
		case readOK:
			return true
		case readSkip:
			continue
		case readBroken:
			r.Close()
			return false
		}
	}

	r.Close()
	return false
}

type readStatus uint8

const (
	readOK readStatus = iota
	readSkip
	readBroken
)

func (r *Reader) read(snapshot *Snapshot) readStatus {
	item := r.iter.Item()
	time := readUnixBE(item.Key())

	if r.start > 0 && r.start > time {
		return readBroken
	}

	// Skip if the current item is within the current frame.
	if r.prev > 0 && time > r.prev {
		r.iter.Next()
		return readSkip
	}

	err := item.Value(func(v []byte) error { return json.Unmarshal(v, snapshot) })
	if err != nil {
		return readBroken
	}

	// Update the timestamp.
	snapshot.Time = time

	// Get the next tick from the current time, whichever is further.
	r.prev -= r.prec
	if prev := time - r.prec; prev < r.prev {
		r.prev = prev
	}

	// Seek for the next call.
	r.iter.Next()

	return readOK
}
