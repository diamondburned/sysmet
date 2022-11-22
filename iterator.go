package sysmet

import (
	"bytes"
	"log"
	"math"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

// IteratorOpts is the options for reading. It describes the range of data to
// read.
type IteratorOpts struct {
	// From is the time to start reading the metrics backwards. The default
	// zero-value means to read from the latest point.
	From time.Time
	// To is the time to stop reading the metrics backwards. By default, the
	// zero-value is used, which would read all metrics. The To time must
	// ALWAYS be before From.
	To time.Time
}

// Iterator is a backwards metric iterator that allows iterating over points.
type Iterator struct {
	tx *badger.Txn
	it *badger.Iterator

	// current state
	item  *badger.Item
	error error

	// constants
	begin []byte
	end   []byte
	to    uint32
	from  uint32
}

// newIterator creates a new iterator. See (*Database).Iterator.
func newIterator(db *badger.DB, opts IteratorOpts) (*Iterator, error) {
	if !opts.To.IsZero() && !opts.From.IsZero() {
		if !opts.From.After(opts.To) {
			return nil, errors.New("opts.From should be after opts.To")
		}
	}

	i := Iterator{
		to:   convertWithUnixZero(opts.To),
		from: convertWithUnixZero(opts.From),
	}

	// If from is 0, then we start at the end of time.
	if i.from == 0 {
		i.begin = bkey(bPoints, unixToBE(math.MaxUint32))
	} else {
		i.begin = bkey(bPoints, unixToBE(i.from))
	}

	// If to is 0, then we end at the beginning of time. This is the default.
	i.end = bkey(bPoints, unixToBE(i.to))

	i.tx = db.NewTransaction(false)
	i.it = i.tx.NewIterator(badger.IteratorOptions{
		Prefix:  bkey(bPoints),
		Reverse: true, // from is later than to
	})

	i.Rewind()

	return &i, nil
}

// Close closes the reader.
func (i *Iterator) Close() error {
	i.it.Close()
	i.tx.Discard()
	return nil
}

func (i *Iterator) setItem() {
	if i.it.Valid() {
		i.item = i.it.Item()
	} else {
		i.item = nil
	}
}

func (i *Iterator) itemTime() uint32 {
	key := bkeyTrim(i.item.Key(), bPoints)
	return readUnixBE(key)
}

func (i *Iterator) seek(t uint32) {
	i.it.Seek(bkey(bPoints, unixToBE(t)))
	i.setItem()
}

// isValid returns true if the reader is still valid.
func (i *Iterator) isValid() bool {
	if i.item == nil {
		return false
	}

	if i.to == 0 || i.to <= i.itemTime() {
		return true
	}

	return false
}

// Prev reads the previous item into the given snapshot pointer or the last item
// if the Reader has never been used before. If snapshot is nil, then the
// iterator is still updated, but no unmarshaling is done.
//
// False is returned if nothing is read and the reader is closed, otherwise true
// is.
func (i *Iterator) Prev(snapshot *Snapshot) bool {
	if !i.isValid() {
		i.item = nil
		return false
	}

	if snapshot != nil {
		if !i.readSnapshot(snapshot) {
			i.item = nil
			return false
		}
	}

	// Seek for the next call.
	i.it.Next()
	i.setItem()

	return true
}

func (i *Iterator) readSnapshot(snapshot *Snapshot) bool {
	// Unmarshal fail is a fatal error, so we invalidate everything.
	if err := i.item.Value(func(v []byte) error {
		return decodeSnapshot(v, snapshot)
	}); err != nil {
		i.error = err
		log.Println("readSnapshot failed:", i.error)
		return false
	}

	// Update the timestamp.
	snapshot.time = i.itemTime()

	return true
}

// Remaining returns the number of remaining keys to read until either the
// database has nothing left or the requested range has been reached. The cursor
// position stays the same by the time this function returns.
func (i *Iterator) Remaining() int {
	// Remember the current cursor position before we change it, because we'll
	// need to preserve this. We'll also have to copy the key, because the
	// iterator will reuse the same buffer.
	current := append([]byte(nil), i.item.Key()...)

	// Attempt to precalculate the number of snapshots to read.
	var total int
	for i.Prev(nil) {
		total++
	}

	// Seek back to where we were.
	i.it.Rewind()
	i.it.Seek(current)
	if !i.it.Valid() {
		log.Panicln("Remaining: cannot seek back to last known key")
	}

	i.setItem()
	if !bytes.Equal(i.item.Key(), current) {
		log.Panicf("Remaining: cannot seek back to last known key: known %d != new %d",
			current, readUnixBE(i.it.Item().Key()))
	}

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
	i.it.Rewind()
	i.it.Seek(i.begin)
	i.setItem()
}

// ReadAll is similar to ReadRemaining, except the cursor is rewound to the
// requested position "from" and read again.
func (i *Iterator) ReadAll() []Snapshot {
	i.Rewind()
	return i.ReadRemaining()
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
func (i *Iterator) ReadExact(precision time.Duration) (SnapshotBuckets, error) {
	return i.readExact(precision, false)
}

// ReadBucketEdges behaves similarly to ReadExact, except each bucket will only
// have the latest point. This is great when data has to be read over a wide
// range of time.
func (i *Iterator) ReadBucketEdges(precision time.Duration) (SnapshotBuckets, error) {
	return i.readExact(precision, true)
}

func (i *Iterator) readExact(precision time.Duration, last bool) (SnapshotBuckets, error) {
	if i.from == 0 || i.to == 0 {
		return SnapshotBuckets{}, nil
	}

	from := float64(i.from)
	prec := float64(precision) / float64(time.Second)
	blen := int(math.Ceil(float64(i.from-i.to) / prec))

	buckets := SnapshotBuckets{
		Range: BucketRange{
			From: time.Unix(int64(i.from), 0),
			To:   time.Unix(int64(i.to), 0),
			Prec: precision,
		},
		Buckets: make([]SnapshotBucket, blen),
	}

	if !last {
		buckets.Snapshots = i.ReadAll()
	} else {
		buckets.Snapshots = make([]Snapshot, blen)
		prev := i.from // from == end, to == start
		prec := uint32(prec)

		// Reset the iterator to the end, then seek it to where we want to go.
		i.it.Rewind()

		for i.seek(prev); blen > 0 && i.isValid(); i.seek(prev) {
			key := bkeyTrim(i.item.Key(), bPoints)
			time := readUnixBE(key)

			if !i.readSnapshot(&buckets.Snapshots[blen-1]) {
				return SnapshotBuckets{}, i.error
			}
			blen--

			prev -= prec
			time -= prec
			// Get the next tick from the current time, whichever is further.
			if time < prev {
				prev = time
			}
		}
	}

	// Exit if we have no snapshots.
	if len(buckets.Snapshots) == 0 {
		return buckets, i.error
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

	for snapshotIx >= 0 && bucketIx >= 0 {
		snapshot := buckets.Snapshots[snapshotIx]

		// Test if the snapshot is within the bucket.
		if uint32(bucketTo) < snapshot.time && snapshot.time <= uint32(bucketFrom) {
			snapshotIx--
			continue
		}

		cutBucket()
	}

	// Ensure the last bucket is already filled when we're out of snapshots.
	if bucketIx >= 0 {
		cutBucket()
	}

	return buckets, i.error
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

// FillGaps fills the buckets with the given multiplier for determine the
// threshold. A good gapMult value is 0.10.
func (buckets *SnapshotBuckets) FillGaps(gapPerc float64) {
	threshold := buckets.GapThreshold(gapPerc)
	nonEmpty := len(buckets.Buckets) - 1

	cpy := func(from int) {
		for j := from + 1; j < nonEmpty; j++ {
			buckets.Buckets[j] = buckets.Buckets[nonEmpty]
		}
		nonEmpty = from
	}

	for i := nonEmpty; i >= 0; i-- {
		// Skip empty buckets (we're only counting in-between non-empty buckets)
		// unless we're at the end.
		if len(buckets.Buckets[i].Snapshots) == 0 && i != 0 {
			continue
		}

		if gap := nonEmpty - i; 0 < gap && gap < threshold {
			// Non-empty snapshot found. Copy from the last non-empty to this
			// one if we're within the threshold.
			cpy(i)
		}
	}
}

// GapThreshold returns the threshold that determine a gap after n empty
// buckets. The given perc variable determines the percentage from 0 to 1 that
// determines after how many empty buckets should be treated as a gap.
func (buckets SnapshotBuckets) GapThreshold(perc float64) int {
	return int(math.Round(float64(len(buckets.Buckets)) * perc))
}

// Below lies the magical mean/std-dev method.

// func (buckets SnapshotBuckets) GapThreshold(mult float64) int {
// 	if mult <= 1.0 {
// 		return int(math.Ceil(float64(len(buckets.Buckets)) * mult))
// 	}
//
// 	// Calculate the frequencies of all gaps.
// 	gaps := make([]float64, 1, len(buckets.Buckets))
//
// 	for i := len(buckets.Buckets) - 1; i >= 0; i-- {
// 		if len(buckets.Buckets[i].Snapshots) == 0 {
// 			gaps[len(gaps)-1]++
// 			continue
// 		}
//
// 		if gaps[len(gaps)-1] != 0 {
// 			gaps = append(gaps, 0)
// 		}
// 	}
//
// 	// Calculate the moving average of spike frequencies to derive a threshold
// 	// to which we should determine a data is gapped.
// 	// Algorithm ported from https://stats.stackexchange.com/a/56744.
// 	gapMean, _ := stats.Mean(gaps)
// 	gapStdDev, _ := stats.StandardDeviationSample(gaps)
// 	gapThreshold := math.Ceil(gapMean + gapStdDev*mult)
//
// 	return int(gapThreshold)
// }
