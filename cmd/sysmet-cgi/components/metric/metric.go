// Package metric draws metric graphs using SVG lines. it is primarily taken
// from zserge/metric.
package metric

import (
	"embed"
	"fmt"
	"html/template"
	"io"
	"math"
	"strings"
)

var (
	//go:embed *.html
	htmls embed.FS
	tmpls *template.Template

	funcs = template.FuncMap{
		"path": path,
		"revInts": func(length int) []int {
			ixs := make([]int, length)
			for i := range ixs {
				ixs[i] = len(ixs) - i - 1
			}
			return ixs
		},
	}
)

func init() {
	tmpls = template.New("metric")
	tmpls = tmpls.Funcs(funcs)
	tmpls = template.Must(tmpls.ParseFS(htmls, "*"))
}

// GraphData contains the data for graphing. Width and Height are optional.
type GraphData struct {
	Samples   [][]float64
	MaxSample float64 // either must be non-0
	MinSample float64
	Symmetric bool
}

// Graph renders an SVG graph with the given data. The default width and height
// is 150x35.
func Graph(w io.Writer, data GraphData) error {
	return tmpls.ExecuteTemplate(w, "graph", data)
}

const (
	pathHeight = 100
	pathWidth  = 25 - 2
)

// NoFloat is the sentinel value to make path find the max/min number.
var NoFloat = math.NaN()

func path(data GraphData, samples []float64) template.HTMLAttr {
	paths := strings.Builder{}
	paths.Grow(10 * 1024) // 10KB
	paths.WriteString("d=\"")

	min := data.MinSample
	max := data.MaxSample

	if min == NoFloat || max == NoFloat {
		for _, x := range samples {
			if min == NoFloat || x < min {
				min = x
			}
			if max == NoFloat || x > max {
				max = x
			}
		}
	}

	// If th data is symmetric, then the minimum and maximum should be the same
	// and is the biggest of the two.
	if data.Symmetric {
		if min > max {
			max = min
		} else {
			min = max
		}
	}

	for i, v := range samples {
		x := float64(i+1) / float64(len(samples))
		y := (v - min) / (max - min)
		if max == min {
			y = 0
		}

		if i == 0 {
			fmt.Fprintf(&paths, "M%f %f", 0.0, (1-y)*pathWidth+1)
		}

		fmt.Fprintf(&paths, " L%f %f", x*pathHeight, (1-y)*pathWidth+1)
	}

	paths.WriteByte('"')

	return template.HTMLAttr(paths.String())
}
