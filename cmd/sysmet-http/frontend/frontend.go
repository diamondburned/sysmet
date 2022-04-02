package frontend

import (
	"embed"
	"encoding/json"
	"html/template"
	"io"
	"io/fs"
	"log"
	"net/http"
	"time"

	"git.unix.lgbt/diamondburned/sysmet"
	"github.com/diamondburned/tmplutil"
	"github.com/pkg/errors"
)

//go:embed *
var webFS embed.FS

var Templater = tmplutil.Templater{
	FileSystem: webFS,
	Includes: map[string]string{
		"rawcss": "static/style.css",
	},
	Functions: template.FuncMap{},
}

func init() {
	// tmplutil.Log = true
	tmplutil.Preregister(&Templater)
}

// MountStatic mounts a static HTTP handler.
func MountStatic() http.Handler {
	sub, err := fs.Sub(webFS, "static")
	if err != nil {
		log.Panicln("failed to get static:", err)
	}

	return http.FileServer(http.FS(sub))
}

var noBuckets = sysmet.SnapshotBuckets{}

// PointsPerGraph defines the number of points per graph with respect to the
// total duration.
const PointsPerGraph = 200

// ReadSnapshots reads snapshots from the given path with the given duration.
func ReadSnapshots(path string, dura time.Duration) (sysmet.SnapshotBuckets, error) {
	now := time.Now()

	d, err := sysmet.Open(path, false)
	if err != nil {
		return noBuckets, errors.Wrap(err, "failed to open")
	}
	defer d.Close()

	opts := sysmet.IteratorOpts{
		From: now,
		To:   now.Add(-dura),
	}

	iter, err := d.Iterator(opts)
	if err != nil {
		return noBuckets, errors.Wrapf(err, "failed to read frame %v", opts)
	}
	defer iter.Close()

	buckets, err := iter.ReadBucketEdges(dura / PointsPerGraph)
	if err != nil {
		return buckets, errors.Wrap(err, "cannot read buckets")
	}

	buckets.FillGaps(0.15)

	return buckets, nil
}

// WriteJSON writes snapshots as JSON. Errors are logged into stderr.
func WriteJSON(w io.Writer, path string, dura time.Duration) {
	snapshots, err := ReadSnapshots(path, dura)
	if err != nil {
		log.Println("failed to write JSON:", err)
		return
	}

	json.NewEncoder(w).Encode(snapshots)
}
