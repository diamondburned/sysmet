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
		if err := os.RemoveAll(path); err != nil {
			t.Error("failed to clean up db:", err)
		}
	})

	t.Cleanup(func() {
		if err := db.gc(db.start+20, 10); err != nil {
			t.Fatal("failed to gc:", err)
		}

		r, err := db.Read(ReadOpts{})
		if err != nil {
			t.Fatal("failed to create reader after GC:", err)
		}
		defer r.Close()

		if snapshotNum := len(r.ReadAll()); snapshotNum != 10 {
			t.Fatalf("expected %d snapshots after GC, got %d", 10, snapshotNum)
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

	r, err := db.Read(ReadOpts{
		Start:     time.Unix(int64(db.start-10), 0),
		End:       time.Unix(int64(db.start+20), 0),
		Precision: 5 * time.Second,
	})
	if err != nil {
		t.Fatal("failed to create reader:", err)
	}
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
	r, err := db.Read(ReadOpts{
		Start:     time.Unix(int64(db.start), 0),
		End:       time.Unix(int64(db.start+20), 0),
		Precision: time.Second,
	})
	if err != nil {
		t.Fatal("failed to create reader:", err)
	}
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

	r, err := db.Read(ReadOpts{
		// Go backwards from the last 10s to the last 2s with 2s accuracy. With
		// 20 points for 20 seconds, 1 each, this should give us 4 points.
		Start:     time.Unix(int64(db.start), 0).Add(10 * time.Second),
		End:       time.Unix(int64(db.start), 0).Add(18 * time.Second),
		Precision: 2 * time.Second,
	})
	if err != nil {
		t.Fatal("failed to create reader:", err)
	}
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
