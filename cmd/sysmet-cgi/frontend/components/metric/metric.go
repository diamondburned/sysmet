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
	frontend.Templater.Func("drawPaths", drawPaths)
	frontend.Templater.Func("prepareGraph", prepareGraph)
	frontend.Templater.Func("renderGraphHovers", renderGraphHovers)
}

// GraphData contains the data for graphing. Width and Height are optional.
type GraphData struct {
	// Names contains a list of names that correspond to each Samples sets.
	Names []string
	// Colors contains a list of colors that correspond to Names. If there are
	// more names than colors, then those names will be colored black.
	Colors []uint32
	// Samplesets contains a sampleset list of samples. Sample points can be
	// NaN, in which it will not be drawn out. If samplesets don't have equal
	// lengths, then an error will be returned and the graph will not be drawn.
	Samplesets [][]float64
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
}

// NewGraphData creates a new graph data with reasonable defaults.
func NewGraphData(b sysmet.SnapshotBuckets, height int, names ...string) GraphData {
	return GraphData{
		Names:      names,
		Samplesets: newSamples(b, len(names)),
		PtString:   FormatDecimalPlaces(3),
		Range:      b.Range,
		MaxSample:  AutoValue,
		MinSample:  0,
		Width:      float64(b.Range.From.Sub(b.Range.To) / b.Range.Prec),
		Height:     float64(height),
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
	if len(data.Samplesets) > 0 && n != len(data.Samplesets[0]) {
		panic("n mismatches len(data.Samples[0])")
	}

	ix := len(data.Names)
	sm := make([]float64, n)
	for i := range sm {
		sm[i] = NaN
	}

	data.Names = append(data.Names, name)
	data.Samplesets = append(data.Samplesets, sm)

	return ix
}

// PointInString calls data.PtString if pt is a number. If pt is NaN, then
// "null" is returned.
func (data *GraphData) PointInString(pt float64) string {
	if math.IsNaN(pt) {
		return "null"
	}

	return data.PtString(pt)
}

// ColorHex returns the name's color in CSS hexadecimal format. It returns an
// empty string if the name isn't in the colors list.
func (data *GraphData) ColorHex(nameIx int) template.HTMLAttr {
	if nameIx >= len(data.Colors) {
		return "#000"
	}

	return template.HTMLAttr(fmt.Sprintf("#%06X", data.Colors[nameIx]))
}

// MidSample returns the middle sample between min and max.
func (data *GraphData) MidSample() float64 {
	return (data.MaxSample + data.MinSample) / 2
}

// RangeTime returns the time of the sample from the given index.
func (data *GraphData) RangeTime(i int) time.Time {
	if len(data.Samplesets) == 0 {
		return time.Time{}
	}

	duration := data.Range.From.Sub(data.Range.To)
	duration *= time.Duration(i)
	duration /= time.Duration(len(data.Samplesets[0]))

	return data.Range.To.Add(duration)
}

// IsNullAt returns true if all samplesets at the given index contain NaN
// values.
func (data *GraphData) IsNullAt(i int) bool {
	// Bound check; return false if out of bounds.
	if len(data.Samplesets) == 0 || len(data.Samplesets[0]) <= i {
		return false
	}

	for _, samples := range data.Samplesets {
		if !math.IsNaN(samples[i]) {
			return false
		}
	}

	return true
}

// IsLaterHalf returns true if the current width index is beyond half.
func (data *GraphData) IsLaterHalf(i int) bool {
	return i > int(data.Width)/2
}

// FormatDecimalPlaces returns a new PtString formatter that formats a floating
// point number to the given decimal places.
func FormatDecimalPlaces(dec int) func(float64) string {
	f := "%." + strconv.Itoa(dec) + "f"
	return func(v float64) string { return fmt.Sprintf(f, v) }
}

// FormatSigFigs returns a new PtString formatter that formats a floating point
// number to the given significant figures with trailing zeros trimmed out.
func FormatSigFigs(sf int) func(float64) string {
	f := "%." + strconv.Itoa(sf) + "g"
	return func(v float64) string { return fmt.Sprintf(f, v) }
}

// FormatBytes plugs into PtString to format bytes as strings.
func FormatBytes(b float64) string {
	s := humanize.Bytes(uint64(math.Abs(b)))
	if math.Signbit(b) {
		s = "-" + s
	}
	return s
}

// FormatPercentage plugs into PtString to format the float as a percentage. typ
// should be either 'g' or 'f'.
func FormatPercentage(dec int, typ byte) func(float64) string {
	f := "%." + strconv.Itoa(dec) + string(typ) + "%%"
	return func(v float64) string { return fmt.Sprintf(f, v) }
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

func prepareGraph(data GraphData) *graphData {
	if len(data.Samplesets) == 0 {
		return &graphData{data, nil}
	}

	// Ensure that all samples are of equal lengths.
	firstLen := len(data.Samplesets[0])
	for i, samples := range data.Samplesets[1:] {
		if len(samples) != firstLen {
			return &graphData{data, fmt.Errorf(
				"mismatch len %d (first) != %d (%d)",
				firstLen, len(samples), i,
			)}
		}
	}

	if data.Width == 0 {
		for _, samples := range data.Samplesets {
			if w := float64(len(samples)); w > data.Width {
				data.Width = w
			}
		}
	}

	if math.IsNaN(data.MinSample) || math.IsNaN(data.MaxSample) {
		// Search across all samplesets for the absolute maximum and minimum
		// across all of them.
		for _, samples := range data.Samplesets {
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

	return &graphData{data, nil}
}

func drawPaths(data *graphData) template.HTML {
	html := strings.Builder{}
	html.Grow(8 * 1024) // 8KB

	for i := len(data.Samplesets) - 1; i >= 0; i-- {
		html.WriteString(`<path class="sample-`)
		html.WriteString(strconv.Itoa(i))
		html.WriteString(`" `)

		html.WriteString(`stroke="`)
		html.WriteString(string(data.ColorHex(i)))
		html.WriteString(`" `)

		html.WriteString(`d="`)
		pathD(&html, data, data.Samplesets[i])
		html.WriteString(`" />`)
	}

	return template.HTML(html.String())
}

func pathD(paths *strings.Builder, data *graphData, samples []float64) {
	width := data.Width
	height := data.Height

	length := float64(len(samples))
	offset := data.MaxSample - data.MinSample
	if offset == 0 {
		// ???
		return
	}

	x := 0.0
	y := 0.0
	prev := false

	// TODO: ensure we have points drawn at start and end. End may have 0.

	for i := len(samples) - 1; i >= 0; i-- {
		v := samples[i]

		if math.IsNaN(v) {
			prev = false
			continue
		}

		x = float64(i+1) / length * width
		y = (v - data.MinSample) / offset

		// If we're initially drawing the first point on the SVG, then Move to
		// that point. Otherwise, draw a Line to that point.
		var cmd = 'L'
		if !prev {
			cmd = 'M'
		}

		prev = true

		// Code unwrapped from "%c%.5f %.5f ".
		paths.WriteRune(cmd)
		paths.WriteString(strconv.FormatFloat(x, 'f', 5, 64))
		paths.WriteByte(' ')
		paths.WriteString(strconv.FormatFloat((1-y)*height, 'f', 5, 64))
		paths.WriteByte(' ')
	}
}
