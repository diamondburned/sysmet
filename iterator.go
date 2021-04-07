package sysmet

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

// IteratorOpts is the options for reading. It describes the range of data to
// read.
type IteratorOpts struct {
	// From is the time to start reading the metrics backwards. The default
	// zero-value means to read from the latest point.
	From time.Time
	// To is the time to stop reading the metrics backwards. By default, the
	// zero-value is used, which would read all metrics. The To time must
	// ALWAYS be before End.
	To time.Time
}

// Iterator is a backwards metric iterator that allows iterating over points.
type Iterator struct {
	tx *bbolt.Tx
	cs *bbolt.Cursor

	// current state
	key   []byte
	value []byte

	// constants
	to   uint32
	from uint32
}

// newIterator creates a new iterator. See (*Databse).Iterator.
func newIterator(db *bbolt.DB, opts IteratorOpts) (*Iterator, error) {
	if !opts.To.IsZero() && !opts.From.IsZero() {
		if !opts.From.After(opts.To) {
			return nil, errors.New("opts.From should be after opts.To")
		}
	}

	i := Iterator{
		to:   convertWithUnixZero(opts.To),
		from: convertWithUnixZero(opts.From),
	}

	tx, err := db.Begin(false)
	if err != nil {
		return nil, errors.Wrap(err, "failed to begin tx")
	}

	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		return nil, ErrUninitialized
	}

	i.tx = tx
	i.cs = bucket.Cursor()

	i.Rewind()

	return &i, nil
}

// Close closes the reader.
func (i *Iterator) Close() error {
	return i.tx.Rollback()
}

// step updates the internal cursor backwards.
func (i *Iterator) step() {
	i.key, i.value = i.cs.Prev()
}

// isValid returns true if the reader is still valid.
func (i *Iterator) isValid() bool { return i.key != nil }

// Prev reads the previous item into the given snapshot pointer or the last item
// if the Reader has never been used before. If snapshot is nil, then the
// iterator is still updated, but no unmarshaling is done.
//
// False is returned if nothing is read and the reader is closed, otherwise true
// is.
func (i *Iterator) Prev(snapshot *Snapshot) bool {
	for i.isValid() {
		time := readUnixBE(i.key)

		if i.to > 0 && i.to > time {
			break
		}

		// Skip if the current time is ahead of what we want to read.
		if i.from > 0 && i.from < time {
			i.step()
			continue
		}

		if snapshot != nil {
			// Unmarshal fail is a fatal error, so we invalidate everything.
			if err := json.Unmarshal(i.value, snapshot); err != nil {
				break
			}

			// Update the timestamp.
			snapshot.Time = time
		}

		// Seek for the next call.
		i.step()

		return true
	}

	// Invalidate the key and value.
	i.key = nil
	i.value = nil

	return false
}

// Remaining returns the number of remaining keys to read until either the
// database has nothing left or the requested range has been reached. The cursor
// position stays the same by the time this function returns.
func (i *Iterator) Remaining() int {
	// Remember the current cursor position before we change it, because we'll
	// need to preserve this.
	current := i.key

	// Attempt to precalculate the number of snapshots to read.
	var total int
	for i.Prev(nil) {
		total++
	}

	// Seek back to where we were.
	i.key, i.value = i.cs.Seek(current)

	return total
}

// ReadRemaining reads all of the reader from the current position to end.
func (i *Iterator) ReadRemaining() []Snapshot {
	total := i.Remaining()
	snapshots := make([]Snapshot, total)

	for total > 0 && i.Prev(&snapshots[total-1]) {
		total--
	}

	return snapshots
}

// Rewind resets the cursor back to the initial position.
func (i *Iterator) Rewind() {
	i.key, i.value = i.cs.Last()
}

// ReadAll is similar to ReadRemaining, except the cursor is rewound to the
// requested position "from" and read again.
func (i *Iterator) ReadAll() []Snapshot {
	i.Rewind()
	return i.ReadRemaining()
}

// SnapshotBucket contains a bucket of snapshot timeframes. It is used by
// ReadExact to allow the caller to manually calculate the averages.
type SnapshotBucket struct {
	Snapshots []Snapshot `json:"snapshots"`
}

// SnapshotBuckets contains all snapshots as well as buckets of those snapshots
// over the given time.
type SnapshotBuckets struct {
	Range     BucketRange      `json:"range"`
	Snapshots []Snapshot       `json:"-"`
	Buckets   []SnapshotBucket `json:"buckets"`
}

// BucketRange describes the range of snapshot buckets.
type BucketRange struct {
	// From is always after To.
	From time.Time     `json:"from"`
	To   time.Time     `json:"to"`
	Prec time.Duration `json:"prec"`
}

// ReadExact reads exactly the given time range, meaning the list of buckets
// will describe exactly the requested range and precision. The cursor will
// automatically be rewound back to the "from" position. The function returns a
// zero-value if from or to was 0.
func (i *Iterator) ReadExact(precision time.Duration) SnapshotBuckets {
	if i.from == 0 || i.to == 0 {
		return SnapshotBuckets{}
	}

	from := float64(i.from)
	prec := float64(precision) / float64(time.Second)

	buckets := SnapshotBuckets{
		Range: BucketRange{
			From: time.Unix(int64(i.from), 0),
			To:   time.Unix(int64(i.to), 0),
			Prec: precision,
		},
		Snapshots: i.ReadAll(),
		Buckets:   make([]SnapshotBucket, int(float64(i.from-i.to)/prec)),
	}

	// Exit if we have no snapshots.
	if len(buckets.Snapshots) == 0 {
		return buckets
	}

	snapshotIx := len(buckets.Snapshots) - 1 // iterate last to first
	bucketIx := len(buckets.Buckets) - 1     // iterate last to first

	// Precalculate the bucket ranges, which starts from the end position
	// "from".
	bucketFrom := from
	bucketTo := from - prec

	// Keep track of the right bound for each bucket. The left bound is the
	// current snapshot index.
	right := len(buckets.Snapshots)

	// cutBucket stores the current snapshots into the bucket and resets the
	// indices.
	cutBucket := func() {
		left := snapshotIx + 1
		buckets.Buckets[bucketIx] = SnapshotBucket{
			// Use a smaller slice of the big snapshots slice for the bucket.
			// This saves us a lot of copies and allocations.
			Snapshots: buckets.Snapshots[left:right],
		}

		// Start preparing for the next iteration with a new bucket.

		// Reset the snapshot ranges.
		right = left

		// Calculate the new bucket ranges.
		bucketFrom = bucketTo
		bucketTo -= prec
		bucketIx--
	}

	for snapshotIx >= 0 {
		snapshot := buckets.Snapshots[snapshotIx]

		// Test if the snapshot is within the bucket.
		if uint32(bucketTo) < snapshot.Time && snapshot.Time <= uint32(bucketFrom) {
			snapshotIx--
			continue
		}

		cutBucket()
	}

	// Ensure the last bucket is already filled when we're out of snapshots.
	if bucketIx >= 0 {
		cutBucket()
	}

	return buckets
}
