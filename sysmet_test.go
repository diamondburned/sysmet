package sysmet

import (
	"math/rand"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
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

	opts := badger.DefaultOptions("")
	opts = opts.WithLoggingLevel(badger.ERROR)
	opts.Compression = options.None
	opts.SyncWrites = true
	opts.InMemory = true

	b, err := badger.Open(opts)
	if err != nil {
		t.Fatal("failed to open badger:", err)
	}

	db := testedDatabase{
		Database: Database{
			db:    b,
			write: true,
		},
		start: now,
	}

	for _, snapshot := range snapshots {
		if err := db.Update(snapshot); err != nil {
			t.Fatal("failed to update:", err)
		}
	}

	t.Cleanup(func() {
		if err := db.gc(db.start+20, 10); err != nil {
			t.Fatal("failed to gc:", err)
		}

		snapshots := db.Read(ReadOpts{}).ReadAll()
		if len(snapshots) != 10 {
			t.Fatalf("expected %d snapshots after GC, got %d", 10, len(snapshots))
		}
	})

	return &db
}

func gatherMetrics(t *testing.T, n uint32) ([]Snapshot, uint32) {
	t.Helper()

	snapshots := make([]Snapshot, n)
	now := uint32(time.Now().Unix())

	var s Snapshot
	for i := uint32(1); i <= n; i++ {
		// Mock a timestamp.
		s.Time = now + i
		s.HostStats = &load.MiscStat{Ctxt: int(i)}

		snapshots[i-1] = s
	}

	// Shuffle the slice and ensure integrity.
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(snapshots), func(i, j int) {
		snapshots[i], snapshots[j] = snapshots[j], snapshots[i]
	})

	return snapshots, now
}

// TestSparsePrec tests for a sparse precision.
func TestSparsePrec(t *testing.T) {
	db := prepDB(t)

	r := db.Read(ReadOpts{
		Start:     time.Unix(int64(db.start-10), 0),
		End:       time.Unix(int64(db.start+20), 0),
		Precision: 5 * time.Second,
	})
	defer r.Close()

	snapshots := r.ReadExact()
	expects := []int{0, 0, 5, 10, 15, 20}

	if len(snapshots) != len(expects) {
		t.Fatalf("expected %d snapshots, got %d", len(expects), len(snapshots))
	}

	for i, snapshot := range snapshots {
		if snapshot.HostStats.Ctxt != expects[i] {
			t.Errorf("snapshot %d expected %d, has %d", i, expects[i], snapshot.HostStats.Ctxt)
		}
	}
}

// TestDensePrec tests for the densest (highest) precision.
func TestDensePrec(t *testing.T) {
	db := prepDB(t)

	// Second is the minimum accuracy.
	r := db.Read(ReadOpts{
		Start:     time.Unix(int64(db.start), 0),
		End:       time.Unix(int64(db.start+20), 0),
		Precision: time.Second,
	})
	defer r.Close()

	snapshots := r.ReadExact()

	if len(snapshots) != 20 {
		t.Fatalf("expected %d snapshots, got %d", 20, len(snapshots))
	}

	for i, snapshot := range snapshots {
		if snapshot.Time == 0 {
			t.Error("missing snapshot time", snapshot.Time)
		}
		if snapshot.HostStats.Ctxt != i+1 {
			t.Errorf("snapshot %d expected %d, has %d", i, i+1, snapshot.HostStats.Ctxt)
		}
	}
}

// TestFramed tests for a small frame of snapshots.
func TestFramed(t *testing.T) {
	db := prepDB(t)

	r := db.Read(ReadOpts{
		// Go backwards from the last 10s to the last 2s with 2s accuracy. With
		// 20 points for 20 seconds, 1 each, this should give us 4 points.
		Start:     time.Unix(int64(db.start), 0).Add(10 * time.Second),
		End:       time.Unix(int64(db.start), 0).Add(18 * time.Second),
		Precision: 2 * time.Second,
	})
	defer r.Close()

	snapshots := r.ReadAll()
	expects := []int{10, 12, 14, 16, 18}

	if len(snapshots) != len(expects) {
		t.Fatalf("expected %d snapshots, got %d", len(expects), len(snapshots))
	}

	for i, snapshot := range snapshots {
		if snapshot.Time == 0 {
			t.Error("missing snapshot time", snapshot.Time)
		}
		if snapshot.HostStats.Ctxt != expects[i] {
			t.Errorf("snapshot %d expected %d, has %d", i, expects[i], snapshot.HostStats.Ctxt)
		}
	}
}
