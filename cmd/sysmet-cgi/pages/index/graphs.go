package index

import (
	"time"

	"git.unix.lgbt/diamondburned/sysmet"
	"github.com/shirou/gopsutil/v3/net"
)

// IgnoredNetworks contains the list of ignored network devices. By default, the
// loopback device is ignored.
var IgnoredNetworks = []string{"lo"}

// sumNetworks sums up all network devices' sent and received byte counters.
func sumNetworks(networks []net.IOCountersStat) (sent, recv uint64) {
sumLoop:
	for _, dev := range networks {
		for _, ignored := range IgnoredNetworks {
			if ignored == dev.Name {
				continue sumLoop
			}
		}

		sent += dev.BytesSent
		recv += dev.BytesRecv
	}

	return
}

// SnapshotTime is a single snapshot time.
type SnapshotTime struct {
	Time string
	Dura time.Duration
}

// PointsPerGraph defines the number of points per graph with respect to the
// total duration.
const PointsPerGraph = 100

// SnapshotTimes describes a list of wanted snapshot times.
var SnapshotTimes = []SnapshotTime{
	{"1 hour", time.Hour},
	{"24 hours", 1 * 24 * time.Hour},
	{"1 week", 7 * 24 * time.Hour},
}

type graphData struct {
	Names  []string // equal lengths
	Points [][]float64
	MinPt  float64
	MaxPt  float64
}

type graphFlattenFunc = func([]sysmet.Snapshot) graphData

var graphFlattenNames = []string{
	"CPU Usage",
	"RAM Usage",
	"Load Average",
	"Network",
	"Disks",
}

var graphFlatteners = map[string]graphFlattenFunc{
	"CPU Usage": func(snapshots []sysmet.Snapshot) graphData {
		graphData := graphData{
			Names:  []string{"User", "System"},
			Points: newPoints(snapshots, 2),
		}

		for i := len(snapshots) - 1; i >= 0; i-- {
			for _, cpu := range snapshots[i].CPUs {
				graphData.Points[0][i] = cpu.User
				graphData.Points[1][i] = cpu.System
			}
		}

		return graphData
	},
	"RAM Usage": func(snapshots []sysmet.Snapshot) graphData {
		graphData := graphData{
			Names:  []string{"RAM", "Swap"},
			MinPt:  0,
			MaxPt:  1,
			Points: newPoints(snapshots, 2),
		}

		for i := len(snapshots) - 1; i >= 0; i-- {
			m := snapshots[i].Memory
			graphData.Points[0][i] = m.UsedPercent / 100
			graphData.Points[1][i] = float64(m.SwapTotal-m.SwapFree) / float64(m.SwapTotal)
		}

		return graphData
	},
	"Load Average": func(snapshots []sysmet.Snapshot) graphData {
		graphData := graphData{
			Names:  []string{"1 minute", "5 minutes", "15 minutes"},
			Points: newPoints(snapshots, 3),
		}

		for i := len(snapshots) - 1; i >= 0; i-- {
			graphData.Points[0][i] = snapshots[i].LoadAvgs.Load1
			graphData.Points[1][i] = snapshots[i].LoadAvgs.Load5
			graphData.Points[2][i] = snapshots[i].LoadAvgs.Load15
		}

		return graphData
	},
	"Network": func(snapshots []sysmet.Snapshot) graphData {
		if len(snapshots) == 0 {
			return graphData{}
		}

		graphData := graphData{
			Names:  []string{"Received", "Sent"},
			Points: newPoints(snapshots, 2),
		}

		// Keep track of the last counters between entries in the snapshots,
		// because the bytes counts are cumulative.
		lastSent, lastRecv := sumNetworks(snapshots[0].Network)

		for i := len(snapshots) - 1; i >= 0; i-- {
			currSent, currRecv := sumNetworks(snapshots[i].Network)

			graphData.Points[0][i] = float64(currRecv - lastRecv)
			graphData.Points[1][i] = float64(currSent - lastSent)

			lastSent, lastRecv = currSent, currRecv
		}

		return graphData
	},
	"Disks": func(snapshots []sysmet.Snapshot) graphData {
		graphData := graphData{
			Names:  []string{}, // dynamically add as we go
			Points: [][]float64{},
			MinPt:  0,
			MaxPt:  100,
		}

		for i := len(snapshots) - 1; i >= 0; i-- {
			for _, disk := range snapshots[i].Disks {
				// Ensure the disk is inside graphData.
				var dataIx = -1
				for j, name := range graphData.Names {
					if name == disk.Path {
						dataIx = j
						break
					}
				}
				if dataIx == -1 {
					dataIx = len(graphData.Names)
					graphData.Names = append(graphData.Names, disk.Path)
					graphData.Points = append(graphData.Points, make([]float64, len(snapshots)))
				}

				graphData.Points[dataIx][i] = disk.UsedPercent
			}
		}

		return graphData
	},
}

func newPoints(snapshots []sysmet.Snapshot, n int) [][]float64 {
	pts := make([][]float64, n)
	for i := range pts {
		pts[i] = make([]float64, len(snapshots))
	}
	return pts
}
