package index

import (
	"fmt"
	"strings"
	"time"

	"git.unix.lgbt/diamondburned/sysmet"
	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend/components/metric"
	"github.com/dustin/go-humanize"
	"github.com/shirou/gopsutil/v3/cpu"
)

// ignoredNetworks contains the list of ignored network devices. By default, the
// loopback device is ignored.
var ignoredNetworks = []string{
	"lo",
}

// IgnoreNetwork ignores a network device. This function is not thread-safe; it
// should only be called on init or main.
func IgnoreNetwork(dev string) {
	ignoredNetworks = append(ignoredNetworks, dev)
}

// sumNetworks sums up all network devices' sent and received byte counters.
func sumNetworks(bucket sysmet.SnapshotBucket) (sent, recv float64) {
	for _, snapshot := range bucket.Snapshots {
	networkLoop:
		for _, dev := range snapshot.Network {
			for _, ignored := range ignoredNetworks {
				if ignored == dev.Name {
					continue networkLoop
				}
			}

			if f := float64(dev.BytesSent); f > sent {
				sent = f
			}
			if f := float64(dev.BytesRecv); f > recv {
				recv = f
			}
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
		var sumActive, sumTotal float64

		for _, cpu := range snapshot.CPUs {
			active := sumCPUTime(cpu)

			sumActive += active
			sumTotal += active + cpu.Idle
		}

		if sumActive > active {
			active = sumActive
			total = sumTotal
		}
	}

	return
}

func sumCPUTime(t cpu.TimesStat) float64 {
	return t.User + t.System + t.Nice + t.Iowait + t.Irq + t.Softirq + t.Steal
}

type snapshotAccessFunc = func(sysmet.Snapshot) float64

// maxOfBucketsInto calculates the means of each getter's accumulated value in
// the current bucket into the given sums slice.
func maxOfBucketsInto(b sysmet.SnapshotBucket, gets []snapshotAccessFunc, sums []float64) {
	for _, bucket := range b.Snapshots {
		for i, get := range gets {
			if val := get(bucket); val > sums[i] {
				sums[i] = val
			}
		}
	}
}

func fillMaxs(
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

		maxOfBucketsInto(bucket, gets, sums)

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

// GraphHeight controls the graph height.
const GraphHeight = 28

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
		graphData.PtString = metric.FormatPercentage(2, 'g')

		// Magic formulas taken from
		// https://github.com/influxdata/telegraf/blob/master/plugins/inputs/cpu/cpu.go#L108

		lastActive := metric.NaN
		lastTotal := metric.NaN

		for i := len(buckets.Buckets) - 1; i >= 0; i-- {
			// Calculate the sum of all cores into one core, then sum up all of
			// that core's times.
			active, total := cpuUsageBuckets(buckets.Buckets[i])

			// NaN is NOT larger than NaN.
			if lastActive > active && lastTotal > total {
				graphData.Samplesets[0][i] = (lastActive - active) / (lastTotal - total) * 100
			}

			lastActive, lastTotal = active, total
		}

		return graphData
	},
	"RAM Usage": func(buckets sysmet.SnapshotBuckets) metric.GraphData {
		graphData := metric.NewGraphData(buckets, GraphHeight, "RAM", "Swap")
		graphData.Colors = []uint32{0xFF9830, 0x5794F2} // orange, blue
		graphData.PtString = metric.FormatBytes

		if len(buckets.Snapshots) > 0 {
			last := buckets.Snapshots[len(buckets.Snapshots)-1]
			graphData.Names[0] += fmt.Sprintf("\t(total %s)", humanize.Bytes(last.Memory.Total))
			graphData.Names[1] += fmt.Sprintf("\t(total %s)", humanize.Bytes(last.Swap.Total))
		}

		fillMaxs(buckets, graphData.Samplesets, []snapshotAccessFunc{
			func(s sysmet.Snapshot) float64 { return float64(s.Memory.Used) },
			func(s sysmet.Snapshot) float64 { return float64(s.Swap.Used) },
		})

		return graphData
	},
	"Load Average": func(buckets sysmet.SnapshotBuckets) metric.GraphData {
		labels := []string{"1 minute", "5 minutes", "15 minutes"}
		graphData := metric.NewGraphData(buckets, GraphHeight, labels...)
		graphData.Colors = []uint32{0x8AC3FF, 0x459AEA, 0x0071D5} // light to dark shades of blue
		graphData.PtString = metric.FormatSigFigs(3)

		fillMaxs(buckets, graphData.Samplesets, []snapshotAccessFunc{
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

			if lastSent > currSent && lastRecv > currRecv {
				graphData.Samplesets[0][i] = lastSent - currSent
				graphData.Samplesets[1][i] = lastRecv - currRecv
			}

			lastSent, lastRecv = currSent, currRecv
		}

		return graphData
	},
	"Disks": func(buckets sysmet.SnapshotBuckets) metric.GraphData {
		graphData := metric.NewGraphData(buckets, GraphHeight)
		graphData.PtString = metric.FormatPercentage(0, 'f')
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
						// Awful hack. Truly awful hack.
						if strings.HasPrefix(name, disk.Path+"\t") {
							dataIx = j
							break
						}
					}

					if dataIx == -1 {
						name := fmt.Sprintf(
							"%s\t(total %s)",
							disk.Path, humanize.Bytes(disk.Total),
						)
						dataIx = graphData.AddSamples(name, len(buckets.Buckets))
					}

					graphData.Samplesets[dataIx][i] = disk.UsedPercent
				}
			}
		}

		return graphData
	},
}
