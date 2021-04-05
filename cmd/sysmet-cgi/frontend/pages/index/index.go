package index

import (
	"io"
	"net/http"
	"time"

	"git.unix.lgbt/diamondburned/sysmet"
	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend"
	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend/components/metric"
)

var index = frontend.Templater.Register("index", "pages/index/index.html")

type renderData struct {
	DBPath  string
	Dura    time.Duration // rounded to seconds
	Refresh bool
	Error   error
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
	buckets, err := frontend.ReadSnapshots(r.DBPath, r.Dura)
	r.Error = err
	return buckets
}

// Render renders the index page asynchronously.
func Render(w io.Writer, r *http.Request, dbPath string, d time.Duration) {
	index.Execute(w, &renderData{
		DBPath:  dbPath,
		Refresh: r.FormValue("refresh") != "",
		Dura:    d.Round(time.Second),
	})
}
