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
}

// prepDB prepares a database with 20 datapoints over the span of a second. The
// host stats' ctxt is used as the index.
func prepDB(t *testing.T) *testedDatabase {
	t.Helper()

	snapshots, now := gatherMetrics(t, 20)

	tmpDir := os.TempDir()
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
		if err := db.gc(db.start+20, 10); err != nil {
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

func gatherMetrics(t *testing.T, n uint32) ([]Snapshot, uint32) {
	t.Helper()

	snapshots := make([]Snapshot, n)
	now := uint32(time.Now().Unix())

	for i := uint32(1); i <= n; i++ {
		snapshots[i-1] = Snapshot{
			// Mock a timestamp.
			Time:      now + i,
			HostStats: load.MiscStat{Ctxt: int(i)},
		}
	}

	// Shuffle the slice and ensure integrity.
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(snapshots), func(i, j int) {
		snapshots[i], snapshots[j] = snapshots[j], snapshots[i]
	})

	return snapshots, now
}

// TestReadExact tests exact bucket reads.
func TestReadExact(t *testing.T) {
	db := prepDB(t)

	type test struct {
		name    string
		opts    IteratorOpts
		prec    time.Duration
		expects [][]int
	}

	var tests = []test{{
		name: "outside",
		opts: IteratorOpts{
			From: time.Unix(int64(db.start+110), 0),
			To:   time.Unix(int64(db.start+100), 0),
		},
		prec:    5 * time.Second,
		expects: [][]int{{}, {}},
	}, {
		name: "too_sparse",
		opts: IteratorOpts{
			From: time.Unix(int64(db.start+20), 0),
			To:   time.Unix(int64(db.start), 0),
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
			From: time.Unix(int64(db.start+20), 0),
			To:   time.Unix(int64(db.start), 0),
		},
		prec: 5 * time.Second,
		expects: [][]int{
			{1, 2, 3, 4, 5},
			{6, 7, 8, 9, 10},
			{11, 12, 13, 14, 15},
			{16, 17, 18, 19, 20},
		},
	}, {
		name: "sparse_small",
		opts: IteratorOpts{
			From: time.Unix(int64(db.start+15), 0),
			To:   time.Unix(int64(db.start+5), 0),
		},
		prec: 5 * time.Second,
		expects: [][]int{
			{6, 7, 8, 9, 10},
			{11, 12, 13, 14, 15},
		},
	}, {
		name: "sparse_overbound",
		opts: IteratorOpts{
			From: time.Unix(int64(db.start+30), 0),
			To:   time.Unix(int64(db.start-10), 0),
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
		name: "dense_normal",
		opts: IteratorOpts{
			From: time.Unix(int64(db.start+20), 0),
			To:   time.Unix(int64(db.start), 0),
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
			From: time.Unix(int64(db.start+30), 0),
			To:   time.Unix(int64(db.start-10), 0),
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
			From: time.Unix(int64(db.start+20), 0),
			To:   time.Unix(int64(db.start+15), 0),
		},
		prec: 500 * time.Millisecond,
		expects: [][]int{
			{}, {16},
			{}, {17},
			{}, {18},
			{}, {19},
			{}, {20},
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r, err := db.Iterator(test.opts)
			if err != nil {
				t.Fatal("failed to create reader:", err)
			}
			defer r.Close()

			buckets := r.ReadExact(test.prec)
			expects := test.expects

			if len(buckets.Buckets) != len(expects) {
				t.Fatalf("expected %d buckets, got %d", len(expects), len(buckets.Buckets))
			}

			for i, bucket := range buckets.Buckets {
				expectsSnapshots := expects[i]

				if len(bucket.Snapshots) != len(expectsSnapshots) {
					t.Errorf(
						"bucket %d expected %d snapshots, got %d",
						i, len(expectsSnapshots), len(bucket.Snapshots),
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
	db := prepDB(t)

	type test struct {
		name         string
		opts         IteratorOpts
		expectsRange [2]int // both inclusive
	}

	var tests = []test{{
		name: "small",
		opts: IteratorOpts{
			From: time.Unix(int64(db.start+18), 0),
			To:   time.Unix(int64(db.start+10), 0),
		},
		expectsRange: [2]int{10, 18},
	}, {
		name:         "all",
		opts:         IteratorOpts{},
		expectsRange: [2]int{1, 20},
	}, {
		name: "overbound",
		opts: IteratorOpts{
			From: time.Unix(int64(db.start+30), 0),
			To:   time.Unix(int64(db.start-10), 0),
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
				if snapshot.Time == 0 {
					t.Error("missing snapshot time", snapshot.Time)
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

// // TestFramed tests for a small frame of snapshots.
// func TestFramed(t *testing.T) {
// 	db := prepDB(t)

// 	r, err := db.Read(ReadOpts{
// 		// Go backwards from the last 10s to the last 2s with 2s accuracy. With
// 		// 20 points for 20 seconds, 1 each, this should give us 4 points.
// 		Start:     time.Unix(int64(db.start), 0).Add(10 * time.Second),
// 		End:       time.Unix(int64(db.start), 0).Add(18 * time.Second),
// 		Precision: 2 * time.Second,
// 	})
// 	if err != nil {
// 		t.Fatal("failed to create reader:", err)
// 	}
// 	defer r.Close()

// 	snapshots := r.ReadAll()
// 	expects := []int{10, 12, 14, 16, 18}

// 	if len(snapshots) != len(expects) {
// 		t.Fatalf("expected %d snapshots, got %d", len(expects), len(snapshots))
// 	}

// 	for i, snapshot := range snapshots {
// 		if snapshot.Time == 0 {
// 			t.Error("missing snapshot time", snapshot.Time)
// 		}
// 		if snapshot.HostStats.Ctxt != expects[i] {
// 			t.Errorf("snapshot %d expected %d, has %d", i, expects[i], snapshot.HostStats.Ctxt)
// 		}
// 	}
// }
