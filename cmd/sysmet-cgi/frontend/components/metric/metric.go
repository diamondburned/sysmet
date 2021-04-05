// Package metric draws metric graphs using SVG lines. it is primarily taken
// from zserge/metric.
package metric

import (
	"fmt"
	"html/template"
	"math"
	"strconv"
	"strings"
	"time"

	"git.unix.lgbt/diamondburned/sysmet"
	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend"
	"github.com/dustin/go-humanize"
)

func init() {
	frontend.Templater.Func("path", path)
	frontend.Templater.Func("prepareGraph", prepareGraph)
	frontend.Templater.Func("fwdInts", func(length int) []int {
		ixs := make([]int, length)
		for i := range ixs {
			ixs[i] = i
		}
		return ixs
	})
	frontend.Templater.Func("revInts", func(length int) []int {
		ixs := make([]int, length)
		for i := range ixs {
			ixs[i] = len(ixs) - i - 1
		}
		return ixs
	})
	frontend.Templater.Func("timestamp", func(t time.Time) string {
		return t.Format("2006-01-02 15:04:05")
	})
}

// GraphData contains the data for graphing. Width and Height are optional.
type GraphData struct {
	// Names contains a list of names that correspond to each Samples sets.
	Names []string
	// Colors contains a list of colors that correspond to Names. If there are
	// more names than colors, then those names will be colored black.
	Colors []uint32
	// Samples contains a sampleset list of samples. Sample points can be NaN,
	// in which it will not be drawn out. If samplesets don't have equal
	// lengths, then an error will be returned and the graph will not be drawn.
	Samples [][]float64
	// PtString formats each sample point into a human-readable string.
	PtString func(float64) string
	// Range describes the time range of this graph.
	Range sysmet.BucketRange
	// MaxSample is the maximum sample point. If NaN, then it will be manually
	// found.
	MaxSample float64 // NaN values are changed
	// MinSample is similar to MaxSample but for miminum.
	MinSample float64
	// Width describes the number of points that the graph should have; if it's
	// 0, then the number of samples will be used instead. It is only relative:
	// SVGs can be scaled arbitrarily.
	Width float64
	// Height is the height of the graph. It determines the aspect ratio of the
	// graph.
	Height float64
	// NullGap, if false, will connect all null (NaN) points to the right-handed
	// previous point if it's not null.
	NullGap bool
}

// NewGraphData creates a new graph data with reasonable defaults.
func NewGraphData(b sysmet.SnapshotBuckets, height int, names ...string) GraphData {
	return GraphData{
		Names:     names,
		Samples:   newSamples(b, len(names)),
		PtString:  FormatDecimalPlaces(3),
		Range:     b.Range,
		MaxSample: AutoValue,
		MinSample: 0,
		Width:     float64(b.Range.From.Sub(b.Range.To) / b.Range.Prec),
		Height:    float64(height),
		NullGap:   false,
	}
}

func newSamples(b sysmet.SnapshotBuckets, n int) [][]float64 {
	pts := make([][]float64, n)
	for i := range pts {
		pts[i] = make([]float64, len(b.Buckets))
		// Set all points as NaN by default.
		for j := range pts[i] {
			pts[i][j] = NaN
		}
	}
	return pts
}

// AddSamples adds a new set of samples with the given n length. If n mismatches
// existing samplesets sizes, then it'll panic.
func (data *GraphData) AddSamples(name string, n int) int {
	if len(data.Samples) > 0 && n != len(data.Samples[0]) {
		panic("n mismatches len(data.Samples[0])")
	}

	ix := len(data.Names)
	sm := make([]float64, n)
	for i := range sm {
		sm[i] = NaN
	}

	data.Names = append(data.Names, name)
	data.Samples = append(data.Samples, sm)

	return ix
}

// ColorHex returns the name's color in CSS hexadecimal format. It returns an
// empty string if the name isn't in the colors list.
func (data GraphData) ColorHex(nameIx int) template.HTMLAttr {
	if nameIx >= len(data.Colors) {
		return "#000"
	}

	return template.HTMLAttr(fmt.Sprintf("#%06X", data.Colors[nameIx]))
}

// MidSample returns the middle sample between min and max.
func (data GraphData) MidSample() float64 {
	return (data.MaxSample + data.MinSample) / 2
}

// PointInString calls data.PtString if pt is a number. If pt is NaN, then
// "null" is returned.
func (data GraphData) PointInString(pt float64) string {
	if math.IsNaN(pt) {
		return "null"
	}

	return data.PtString(pt)
}

// RangeTime returns the time of the sample from the given index.
func (data GraphData) RangeTime(i int) time.Time {
	if len(data.Samples) == 0 {
		return time.Time{}
	}

	duration := data.Range.From.Sub(data.Range.To)
	duration *= time.Duration(i)
	duration /= time.Duration(len(data.Samples[0]))

	return data.Range.To.Add(duration)
}

// IsLaterHalf returns true if the current width index is beyond half.
func (data GraphData) IsLaterHalf(i int) bool {
	return i > int(data.Width)/2
}

// FormatDecimalPlaces returns a new PtString formatter that formats a floating
// point number to the given decimal places.
func FormatDecimalPlaces(dec int) func(float64) string {
	f := "%." + strconv.Itoa(dec) + "f%%"
	return func(v float64) string { return fmt.Sprintf(f, v) }
}

// FormatSigFigs returns a new PtString formatter that formats a floating point
// number to the given significant figures with trailing zeros trimmed out.
func FormatSigFigs(sf int) func(float64) string {
	f := "%." + strconv.Itoa(sf) + "g%%"
	return func(v float64) string { return fmt.Sprintf(f, v) }
}

// FormatBytes plugs into PtString to format bytes as strings.
func FormatBytes(b float64) string {
	return humanize.Bytes(uint64(b))
}

// FormatPercentage plugs into PtString to format the float as a percentage.
func FormatPercentage(f float64) string {
	return fmt.Sprintf("%.3g%%", f)
}

// NaN is a float64 not-a-number constant.
var NaN = math.NaN()

// AutoValue is used in min/max sample numbers to make path find the threshold
// dynamically.
var AutoValue = NaN

type graphData struct {
	GraphData
	Error error
}

func prepareGraph(data GraphData) graphData {
	if len(data.Samples) == 0 {
		return graphData{data, nil}
	}

	// Ensure that all samples are of equal lengths.
	firstLen := len(data.Samples[0])
	for i, samples := range data.Samples[1:] {
		if len(samples) != firstLen {
			return graphData{data, fmt.Errorf(
				"mismatch len %d (first) != %d (%d)",
				firstLen, len(samples), i,
			)}
		}
	}

	if data.Width == 0 {
		for _, samples := range data.Samples {
			if w := float64(len(samples)); w > data.Width {
				data.Width = w
			}
		}
	}

	if math.IsNaN(data.MinSample) || math.IsNaN(data.MaxSample) {
		// Search across all samplesets for the absolute maximum and minimum
		// across all of them.
		for _, samples := range data.Samples {
			for _, x := range samples {
				if math.IsNaN(data.MinSample) || x < data.MinSample {
					data.MinSample = x
				}
				if math.IsNaN(data.MaxSample) || x > data.MaxSample {
					data.MaxSample = x
				}
			}
		}
	}

	return graphData{data, nil}
}

func path(data graphData, samples []float64) template.HTMLAttr {
	paths := strings.Builder{}
	paths.Grow(4096) // 4KB

	width := data.Width
	height := data.Height

	offset := data.MaxSample - data.MinSample
	length := float64(len(samples))

	var x, y float64
	var prev = AutoValue // last non-null

	for i := len(samples) - 1; i >= 0; i-- {
		v := samples[i]

		if math.IsNaN(v) {
			// Skip NaN samples (gap points) if NullGap is true.
			if data.NullGap {
				continue
			}

			// Use the previous point if we have any. Otherwise, draw a gap.
			if !math.IsNaN(prev) {
				v = prev
			} else {
				continue
			}
		}

		x = float64(i+1) / length * width
		y = v - data.MinSample

		if offset != 0 {
			y /= offset
		}

		// If we're initially drawing the first point on the SVG, then Move to
		// that point. Otherwise, draw a Line to that point. We know this by
		// checking that prev is not NaN, because it is a valid value once we
		// draw one.
		var cmd = 'L'
		if math.IsNaN(prev) {
			cmd = 'M'
		}

		prev = v
		fmt.Fprintf(&paths, "%c%.4f %.4f ", cmd, x, (1-y)*height)
	}

	return template.HTMLAttr(paths.String())
}
