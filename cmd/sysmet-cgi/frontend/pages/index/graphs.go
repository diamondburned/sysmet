package index

import (
	"math"
	"time"

	"git.unix.lgbt/diamondburned/sysmet"
	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend/components/metric"
	"github.com/shirou/gopsutil/v3/cpu"
)

// ignoredNetworks contains the list of ignored network devices. By default, the
// loopback device is ignored.
var ignoredNetworks = map[string]struct{}{
	"lo": {},
}

// IgnoreNetwork ignores a network device. This function is not thread-safe; it
// should only be called on init or main.
func IgnoreNetwork(dev string) {
	ignoredNetworks[dev] = struct{}{}
}

// sumNetworks sums up all network devices' sent and received byte counters.
func sumNetworks(bucket sysmet.SnapshotBucket) (sent, recv float64) {
	l := float64(len(bucket.Snapshots))

	for _, snapshot := range bucket.Snapshots {
		for _, dev := range snapshot.Network {
			if _, ignore := ignoredNetworks[dev.Name]; ignore {
				continue
			}

			sent += float64(dev.BytesSent) / l
			recv += float64(dev.BytesRecv) / l
		}
	}

	return
}

// cpuUsageBuckets calculates the CPU usage in 100-percentage of the given
// snapshot bucket. The CPU mean times are taken.
func cpuUsageBuckets(bucket sysmet.SnapshotBucket) (active, total float64) {
	if len(bucket.Snapshots) == 0 {
		return metric.NaN, metric.NaN
	}

	for _, snapshot := range bucket.Snapshots {
		for _, cpu := range snapshot.CPUs {
			active += sumCPUTime(cpu)
			total += active + cpu.Idle
		}
	}

	active /= float64(len(bucket.Snapshots))
	total /= float64(len(bucket.Snapshots))

	return
}

func sumCPUTime(t cpu.TimesStat) float64 {
	return t.User + t.System + t.Nice + t.Iowait + t.Irq + t.Softirq + t.Steal
}

type snapshotAccessFunc = func(sysmet.Snapshot) float64

// meansOfBucketsInto calculates the means of each getter's accumulated value in
// the current bucket into the given sums slice.
func meansOfBucketsInto(b sysmet.SnapshotBucket, gets []snapshotAccessFunc, sums []float64) {
	l := float64(len(b.Snapshots))

	for _, bucket := range b.Snapshots {
		for i, get := range gets {
			sums[i] += get(bucket) / l
		}
	}
}

func fillMeans(
	buckets sysmet.SnapshotBuckets,
	points [][]float64, gets []snapshotAccessFunc,
) {
	sums := make([]float64, len(points))

	for i := len(buckets.Buckets) - 1; i >= 0; i-- {
		bucket := buckets.Buckets[i]
		if len(bucket.Snapshots) == 0 {
			continue
		}

		for i := range sums {
			sums[i] = 0
		}

		meansOfBucketsInto(bucket, gets, sums)

		for n := range gets {
			points[n][i] = sums[n]
		}
	}
}

// SnapshotTime is a single snapshot time.
type SnapshotTime struct {
	Time string
	Dura time.Duration
}

const (
	// PointsPerGraph defines the number of points per graph with respect to the
	// total duration.
	PointsPerGraph = 200
	// GraphHeight controls the graph height.
	GraphHeight = 28
)

type graphFlattenFunc = func(sysmet.SnapshotBuckets) metric.GraphData

var graphFlattenNames = []string{
	"CPU Usage",
	"RAM Usage",
	"Load Average",
	"Network",
	"Disks",
}

var graphFlatteners = map[string]graphFlattenFunc{
	"CPU Usage": func(buckets sysmet.SnapshotBuckets) metric.GraphData {
		graphData := metric.NewGraphData(buckets, GraphHeight, "Usage")
		graphData.Colors = []uint32{0xEAB839}
		graphData.PtString = metric.FormatPercentage

		// Magic formulas taken from
		// https://github.com/influxdata/telegraf/blob/master/plugins/inputs/cpu/cpu.go#L108

		lastActive := metric.NaN
		lastTotal := metric.NaN

		for i := len(buckets.Buckets) - 1; i >= 0; i-- {
			// Calculate the sum of all cores into one core, then sum up all of
			// that core's times.
			active, total := cpuUsageBuckets(buckets.Buckets[i])

			if !math.IsNaN(total) {
				graphData.Samples[0][i] = (lastActive - active) / (lastTotal - total) * 100
			}

			lastActive, lastTotal = active, total
		}

		return graphData
	},
	"RAM Usage": func(buckets sysmet.SnapshotBuckets) metric.GraphData {
		graphData := metric.NewGraphData(buckets, GraphHeight, "RAM", "Swap")
		graphData.Colors = []uint32{0xFF9830, 0x5794F2} // orange, blue
		graphData.PtString = metric.FormatPercentage

		fillMeans(buckets, graphData.Samples, []snapshotAccessFunc{
			func(s sysmet.Snapshot) float64 { return s.Memory.UsedPercent },
			func(s sysmet.Snapshot) float64 { return s.Swap.UsedPercent },
		})

		return graphData
	},
	"Load Average": func(buckets sysmet.SnapshotBuckets) metric.GraphData {
		labels := []string{"1 minute", "5 minutes", "15 minutes"}
		graphData := metric.NewGraphData(buckets, GraphHeight, labels...)
		graphData.Colors = []uint32{0x8AC3FF, 0x459AEA, 0x0071D5} // light to dark shades of blue
		graphData.PtString = metric.FormatSigFigs(3)

		fillMeans(buckets, graphData.Samples, []snapshotAccessFunc{
			func(s sysmet.Snapshot) float64 { return s.LoadAvgs.Load1 },
			func(s sysmet.Snapshot) float64 { return s.LoadAvgs.Load5 },
			func(s sysmet.Snapshot) float64 { return s.LoadAvgs.Load15 },
		})

		return graphData
	},
	"Network": func(buckets sysmet.SnapshotBuckets) metric.GraphData {
		if len(buckets.Buckets) == 0 {
			return metric.GraphData{}
		}

		graphData := metric.NewGraphData(buckets, GraphHeight, "Received", "Sent")
		graphData.Colors = []uint32{0xDF2F44, 0x5794F2} // red, blue
		graphData.PtString = metric.FormatBytes

		// Keep track of the last counters between entries in the snapshots,
		// because the bytes counts are cumulative.
		lastSent := metric.NaN
		lastRecv := metric.NaN

		for i := len(buckets.Buckets) - 1; i >= 0; i-- {
			bucket := buckets.Buckets[i]
			if len(bucket.Snapshots) == 0 {
				lastSent = metric.NaN
				lastRecv = metric.NaN
				continue
			}

			currSent, currRecv := sumNetworks(buckets.Buckets[i])

			if !math.IsNaN(lastSent) && !math.IsNaN(lastRecv) {
				graphData.Samples[0][i] = lastSent - currSent
				graphData.Samples[1][i] = lastRecv - currRecv
			}

			lastSent, lastRecv = currSent, currRecv
		}

		return graphData
	},
	"Disks": func(buckets sysmet.SnapshotBuckets) metric.GraphData {
		graphData := metric.NewGraphData(buckets, GraphHeight)
		graphData.PtString = metric.FormatDecimalPlaces(0)
		graphData.MaxSample = 100
		graphData.Colors = []uint32{
			// https://colorswall.com/palette/102/
			0xff0000,
			0xffa500,
			0xffff00,
			0x008000,
			0x0000ff,
			0x4b0082,
			0xee82ee,
		}

		for i := len(buckets.Buckets) - 1; i >= 0; i-- {
			for _, snapshot := range buckets.Buckets[i].Snapshots {
				for _, disk := range snapshot.Disks {
					// Ensure the disk is inside metric.GraphData.
					var dataIx = -1
					for j, name := range graphData.Names {
						if name == disk.Path {
							dataIx = j
							break
						}
					}
					if dataIx == -1 {
						dataIx = graphData.AddSamples(disk.Path, len(buckets.Buckets))
					}

					graphData.Samples[dataIx][i] = disk.UsedPercent
				}
			}
		}

		return graphData
	},
}
