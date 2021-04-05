package index

import (
	"io"
	"time"

	"git.unix.lgbt/diamondburned/sysmet"
	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend"
	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend/components/metric"
	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
)

var index = frontend.Templater.Register("index", "pages/index/index.html")

type renderData struct {
	DBPath string
	Dura   time.Duration // rounded to seconds
	Opts   sysmet.IteratorOpts
	Error  error
}

func (r *renderData) HumanTime() string {
	return humanize.RelTime(r.Opts.From, r.Opts.To, "later", "ago")
}

type graph struct {
	Name string
	Data metric.GraphData
}

// Graphs fetches the list of needed timeframes from the database.
func (r *renderData) Graphs() []graph {
	buckets := r.snapshots()
	if r.Error != nil {
		return nil
	}

	graphs := make([]graph, len(graphFlattenNames))

	for i := range graphs {
		name := graphFlattenNames[i]
		flatten := graphFlatteners[name]

		graphs[i] = graph{
			Name: name,
			Data: flatten(buckets),
		}
	}

	return graphs
}

func (r *renderData) snapshots() sysmet.SnapshotBuckets {
	d, err := sysmet.Open(r.DBPath, false)
	if err != nil {
		r.Error = errors.Wrap(err, "failed to open")
		return sysmet.SnapshotBuckets{}
	}
	defer d.Close()

	iter, err := d.Iterator(r.Opts)
	if err != nil {
		r.Error = errors.Wrapf(err, "failed to read frame %v", r.Opts)
		return sysmet.SnapshotBuckets{}
	}
	defer iter.Close()

	return iter.ReadExact(r.Dura / PointsPerGraph)
}

// Render renders the index page asynchronously.
func Render(w io.Writer, dbPath string, d time.Duration) {
	now := time.Now()

	index.Execute(w, &renderData{
		DBPath: dbPath,
		Dura:   d.Round(time.Second),
		Opts: sysmet.IteratorOpts{
			From: now,
			To:   now.Add(-d),
		},
	})
}
