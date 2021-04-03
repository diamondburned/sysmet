package index

import (
	"embed"
	"html/template"
	"io"
	"log"
	"strings"
	"time"

	"git.unix.lgbt/diamondburned/sysmet"
	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/components/errbox"
	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/components/metric"
	"github.com/pkg/errors"
)

var (
	//go:embed *.html
	htmls embed.FS
	tmpls *template.Template

	funcs = template.FuncMap{
		"graph": func(data graphData) template.HTML {
			builder := strings.Builder{}

			assertNotNil(metric.Graph(&builder, metric.GraphData{
				Samples:   data.Points,
				MaxSample: data.MaxPt,
				MinSample: data.MinPt,
			}))

			return template.HTML(builder.String())
		},
		"errbox": func(err error) template.HTML {
			builder := strings.Builder{}
			assertNotNil(errbox.Render(&builder, err))
			return template.HTML(builder.String())
		},
	}
)

func assertNotNil(err error) {
	if err != nil {
		log.Panicln("unexpected error:", err)
	}
}

func init() {
	tmpls = template.New("index.html")
	tmpls = tmpls.Funcs(funcs)
	tmpls = template.Must(tmpls.ParseFS(htmls, "*"))
}

type renderData struct {
	DBPath string
	Error  error
}

type graph struct {
	Name  string
	Times []graphTime
}

type graphTime struct {
	Time string
	Data graphData
}

// Graphs fetches the list of needed timeframes from the database.
func (r *renderData) Graphs() []graph {
	snapshotTimes := r.snapshots()
	if snapshotTimes == nil {
		return nil
	}

	graphs := make([]graph, len(graphFlattenNames))

	for i := range graphs {
		graphs[i].Name = graphFlattenNames[i]
		graphs[i].Times = make([]graphTime, len(snapshotTimes))

		flatten := graphFlatteners[graphs[i].Name]

		for j, snapshots := range snapshotTimes {
			graphs[i].Times[j] = graphTime{
				Time: SnapshotTimes[j].Time,
				Data: flatten(snapshots),
			}
		}
	}

	return graphs
}

func (r *renderData) snapshots() [][]sysmet.Snapshot {
	now := time.Now()
	snapshots := make([][]sysmet.Snapshot, len(SnapshotTimes))

	d, err := sysmet.Open(r.DBPath, false)
	if err != nil {
		r.Error = errors.Wrap(err, "failed to open")
		return nil
	}

	defer d.Close()

	for i, time := range SnapshotTimes {
		reader, err := d.Read(sysmet.ReadOpts{
			Start:     now.Add(-time.Dura),
			End:       now,
			Precision: time.Dura / PointsPerGraph,
		})
		if err != nil {
			r.Error = errors.Wrapf(err, "failed to read time %s", time.Time)
			return nil
		}

		snapshots[i] = reader.ReadExact()
		reader.Close()
	}

	return snapshots
}

// Render renders the index page asynchronously.
func Render(w io.Writer, dbPath string) error {
	return tmpls.Execute(w, &renderData{
		DBPath: dbPath,
	})
}
