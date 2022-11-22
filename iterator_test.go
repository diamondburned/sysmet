package sysmet

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v3/load"
)

// testedDatabase extends Database to add useful testing information.
type testedDatabase struct {
	Database
	start uint32
	gc    bool
}

// prepDB prepares a database with 20 datapoints over the span of a second. The
// host stats' ctxt is used as the index.
func prepDB(t testing.TB, snapshots []Snapshot, now uint32) *testedDatabase {
	t.Helper()

	if snapshots == nil {
		snapshots = gatherMetrics(t, 20, now)
	}

	tmpDir := t.TempDir()
	f, err := os.CreateTemp(tmpDir, "sysmet-test-")
	if err != nil {
		t.Fatal("failed to mktemp for db:", err)
	}
	path := f.Name()
	f.Close()

	d, err := Open(path, true)
	if err != nil {
		t.Fatal("failed to open db:", err)
	}

	db := testedDatabase{
		Database: *d,
		start:    now,
	}

	for _, snapshot := range snapshots {
		if err := db.Update(snapshot); err != nil {
			t.Fatal("failed to update:", err)
		}
	}

	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error("failed to close db:", err)
		}

		if err := os.RemoveAll(path); err != nil {
			t.Error("failed to clean up db:", err)
		}
	})

	t.Cleanup(func() {
		if !db.gc {
			return
		}

		if err := db.Database.gc(db.start+20, 10); err != nil {
			t.Fatal("failed to gc:", err)
		}

		iter, err := db.Iterator(IteratorOpts{})
		if err != nil {
			t.Fatal("failed to create iterator after GC:", err)
		}
		defer iter.Close()

		if n := iter.Remaining(); n != 10 {
			t.Fatalf("expected %d snapshots after GC, got %d", 10, n)
		}
	})

	return &db
}

func gatherMetrics(t testing.TB, n, now uint32) []Snapshot {
	t.Helper()

	snapshots := make([]Snapshot, n)

	for i := uint32(1); i <= n; i++ {
		snapshots[i-1] = newFakeSnapshot(now, i)
	}

	// Shuffle the slice and ensure integrity.
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(snapshots), func(i, j int) {
		snapshots[i], snapshots[j] = snapshots[j], snapshots[i]
	})

	return snapshots
}

func newFakeSnapshot(start, i uint32) Snapshot {
	return Snapshot{
		// Mock a timestamp.
		time:      start + i,
		HostStats: load.MiscStat{Ctxt: int(i)},
	}
}

func bucketIxs(buckets []SnapshotBucket) [][]int {
	ints := make([][]int, len(buckets))

	for i, bucket := range buckets {
		ints[i] = snapshotIxs(bucket.Snapshots)
	}

	return ints
}

func snapshotIxs(snapshots []Snapshot) []int {
	ints := make([]int, len(snapshots))

	for i, snapshot := range snapshots {
		ints[i] = snapshot.HostStats.Ctxt
	}

	return ints
}

// TestReadExact tests exact bucket reads.
func TestReadExact(t *testing.T) {
	start := uint32(time.Now().Unix())

	type test struct {
		input   []Snapshot
		name    string
		opts    IteratorOpts
		prec    time.Duration
		fill    bool
		last    bool
		expects [][]int
	}

	var tests = []test{{
		name: "outside",
		opts: IteratorOpts{
			From: time.Unix(int64(start+110), 0),
			To:   time.Unix(int64(start+100), 0),
		},
		prec:    5 * time.Second,
		expects: [][]int{{}, {}},
	}, {
		name: "too_sparse",
		opts: IteratorOpts{
			From: time.Unix(int64(start+20), 0),
			To:   time.Unix(int64(start), 0),
		},
		prec: 20 * time.Second,
		expects: [][]int{
			{
				1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
				11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
			},
		},
	}, {
		name: "sparse_normal",
		opts: IteratorOpts{
			From: time.Unix(int64(start+20), 0),
			To:   time.Unix(int64(start), 0),
		},
		prec: 5 * time.Second,
		expects: [][]int{
			{1, 2, 3, 4, 5},
			{6, 7, 8, 9, 10},
			{11, 12, 13, 14, 15},
			{16, 17, 18, 19, 20},
		},
	}, {
		name: "sparse_normal_last",
		opts: IteratorOpts{
			From: time.Unix(int64(start+20), 0),
			To:   time.Unix(int64(start), 0),
		},
		prec: 5 * time.Second,
		last: true,
		expects: [][]int{
			{5},
			{10},
			{15},
			{20},
		},
	}, {
		name: "sparse_small",
		opts: IteratorOpts{
			From: time.Unix(int64(start+15), 0),
			To:   time.Unix(int64(start+5), 0),
		},
		prec: 5 * time.Second,
		expects: [][]int{
			{6, 7, 8, 9, 10},
			{11, 12, 13, 14, 15},
		},
	}, {
		name: "sparse_small_last",
		opts: IteratorOpts{
			From: time.Unix(int64(start+15), 0),
			To:   time.Unix(int64(start+5), 0),
		},
		prec: 5 * time.Second,
		last: true,
		expects: [][]int{
			{10},
			{15},
		},
	}, {
		name: "sparse_overbound",
		opts: IteratorOpts{
			From: time.Unix(int64(start+30), 0),
			To:   time.Unix(int64(start-10), 0),
		},
		prec: 5 * time.Second,
		expects: [][]int{
			{},
			{},
			{1, 2, 3, 4, 5},
			{6, 7, 8, 9, 10},
			{11, 12, 13, 14, 15},
			{16, 17, 18, 19, 20},
			{},
			{},
		},
	}, {
		name: "sparse_overbound_last",
		opts: IteratorOpts{
			From: time.Unix(int64(start+30), 0),
			To:   time.Unix(int64(start-10), 0),
		},
		prec: 5 * time.Second,
		last: true,
		expects: [][]int{
			{},
			{},
			{5},
			{10},
			{15},
			{20},
			{},
			{},
		},
	}, {
		name: "dense_normal",
		opts: IteratorOpts{
			From: time.Unix(int64(start+20), 0),
			To:   time.Unix(int64(start), 0),
		},
		prec: time.Second,
		expects: [][]int{
			{1}, {2}, {3}, {4}, {5},
			{6}, {7}, {8}, {9}, {10},
			{11}, {12}, {13}, {14}, {15},
			{16}, {17}, {18}, {19}, {20},
		},
	}, {
		name: "dense_overbound",
		opts: IteratorOpts{
			From: time.Unix(int64(start+30), 0),
			To:   time.Unix(int64(start-10), 0),
		},
		prec: time.Second,
		expects: [][]int{
			{}, {}, {}, {}, {},
			{}, {}, {}, {}, {},
			{1}, {2}, {3}, {4}, {5},
			{6}, {7}, {8}, {9}, {10},
			{11}, {12}, {13}, {14}, {15},
			{16}, {17}, {18}, {19}, {20},
			{}, {}, {}, {}, {},
			{}, {}, {}, {}, {},
		},
	}, {
		name: "subsecond",
		opts: IteratorOpts{
			From: time.Unix(int64(start+20), 0),
			To:   time.Unix(int64(start+15), 0),
		},
		prec: 500 * time.Millisecond,
		expects: [][]int{
			{}, {16},
			{}, {17},
			{}, {18},
			{}, {19},
			{}, {20},
		},
	}, {
		name: "gapped",
		opts: IteratorOpts{
			From: time.Unix(int64(start+15), 0),
			To:   time.Unix(int64(start), 0),
		},
		prec: 3 * time.Second / 2,
		fill: true,
		input: []Snapshot{
			newFakeSnapshot(start, 0),
			newFakeSnapshot(start, 1),
			newFakeSnapshot(start, 2), //  gap [3, 8], unfilled
			newFakeSnapshot(start, 9),
			newFakeSnapshot(start, 10), // gap [11, 12], filled
			newFakeSnapshot(start, 13),
			newFakeSnapshot(start, 14),
			newFakeSnapshot(start, 15),
		},
		expects: [][]int{
			{1},
			{2},
			{9}, {9}, {9}, // intentional
			{9},
			{10},
			{13}, // 11, 12
			{13},
			{14, 15},
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := prepDB(t, test.input, start)
			db.gc = false

			r, err := db.Iterator(test.opts)
			if err != nil {
				t.Fatal("failed to create reader:", err)
			}
			defer r.Close()

			buckets, err := r.readExact(test.prec, test.last)
			if err != nil {
				t.Fatal("cannot read:", err)
			}

			expects := test.expects

			if test.fill {
				buckets.FillGaps(1)
			}

			if len(buckets.Buckets) != len(expects) {
				t.Fatalf("unexpected buckets:\n"+
					"expected %02d: %v\n"+
					"got      %02d: %v",
					len(expects), expects, len(buckets.Buckets), bucketIxs(buckets.Buckets),
				)
			}

			t.Logf("expects: %v\n", expects)
			t.Logf("got:     %v\n", bucketIxs(buckets.Buckets))

			for i, bucket := range buckets.Buckets {
				expectsSnapshots := expects[i]

				if len(bucket.Snapshots) != len(expectsSnapshots) {
					t.Errorf("unexpected snapshots in bucket %d:\n"+
						"expected %02d: %v\n"+
						"got      %02d: %v\n",
						i,
						len(expectsSnapshots), expectsSnapshots,
						len(bucket.Snapshots), snapshotIxs(bucket.Snapshots),
					)
					continue
				}

				for j, snapshot := range bucket.Snapshots {
					if snapshot.HostStats.Ctxt != expectsSnapshots[j] {
						t.Errorf(
							"snapshot %d in bucket %d expected %d, got %d",
							j, i, expectsSnapshots[j], snapshot.HostStats.Ctxt,
						)
					}
				}
			}
		})
	}
}

func TestReadAll(t *testing.T) {
	start := uint32(time.Now().Unix())
	db := prepDB(t, nil, start)

	type test struct {
		name         string
		opts         IteratorOpts
		expectsRange [2]int // both inclusive
	}

	var tests = []test{{
		name: "small",
		opts: IteratorOpts{
			From: time.Unix(int64(start+18), 0),
			To:   time.Unix(int64(start+10), 0),
		},
		expectsRange: [2]int{10, 18},
	}, {
		name:         "all",
		opts:         IteratorOpts{},
		expectsRange: [2]int{1, 20},
	}, {
		name: "overbound",
		opts: IteratorOpts{
			From: time.Unix(int64(start+30), 0),
			To:   time.Unix(int64(start-10), 0),
		},
		expectsRange: [2]int{1, 20},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r, err := db.Iterator(test.opts)
			if err != nil {
				t.Fatal("failed to create reader:", err)
			}
			defer r.Close()

			snapshots := r.ReadAll()

			test.expectsRange[1]++ // inclusive
			expectsLen := test.expectsRange[1] - test.expectsRange[0]

			if len(snapshots) != expectsLen {
				t.Fatalf("expected %d snapshots, got %d", expectsLen, len(snapshots))
			}

			for i, snapshot := range snapshots {
				if snapshot.time == 0 {
					t.Error("missing snapshot time")
					continue
				}
				if snapshot.HostStats.Ctxt != test.expectsRange[0]+i {
					t.Errorf(
						"snapshot %d expected %d, got %d",
						i, test.expectsRange[0]+i, snapshot.HostStats.Ctxt,
					)
				}
			}
		})
	}
}

func BenchmarkReadAll(b *testing.B) {
	start := uint32(time.Now().Unix())
	db := prepDB(b, nil, start)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := db.Iterator(IteratorOpts{})
		if err != nil {
			b.Fatal("failed to create reader:", err)
		}
		r.ReadAll()
		r.Close()
	}
}
