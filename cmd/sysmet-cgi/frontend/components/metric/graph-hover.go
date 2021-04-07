package metric

import (
	"html/template"
	"strings"
	"time"

	"git.unix.lgbt/diamondburned/sysmet"
	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend"
	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend/components/errbox"
	"github.com/dustin/go-humanize"
)

var graphHover = frontend.Templater.Subtemplate("graph-hover")

type graphHoverData struct {
	Timestamp    string
	RelativeTime string
	HoverRows    []graphHoverRow
}

type graphHoverRow struct {
	Color template.HTMLAttr
	Name  string
	Value string
}

// timestamp formats the given time in HTML datetime format.
func timestamp(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}

// relTime formats the relative time.
func relTime(t time.Time, r sysmet.BucketRange) string {
	return humanize.RelTime(t, r.From, "ago", "later")
}

func renderGraphHovers(data *graphData) template.HTML {
	buf := strings.Builder{}
	buf.Grow(500 * 1024) // 512KB buffer

	hoverData := graphHoverData{
		HoverRows: make([]graphHoverRow, len(data.Samplesets)),
	}

	var lastIx int // keep track of last non-null index

	sampleSize := len(data.Samplesets[0])
	for j := 0; j < sampleSize; j++ {
		if !data.IsNullAt(j) {
			lastIx = j
		}

		time := data.RangeTime(j)
		hoverData.Timestamp = timestamp(time)
		hoverData.RelativeTime = relTime(time, data.Range)

		for i, samples := range data.Samplesets {
			hoverData.HoverRows[i] = graphHoverRow{
				Color: data.ColorHex(i),
				Name:  data.Names[i],
				Value: data.PtString(samples[lastIx]),
			}
		}

		if err := graphHover.Execute(&buf, &hoverData); err != nil {
			// We're skipping the whole buffer here.
			return errbox.Render(err)
		}
	}

	return template.HTML(buf.String())
}
