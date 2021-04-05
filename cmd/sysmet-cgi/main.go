package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/cgi"
	"time"

	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend"
	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend/pages/errpage"
	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend/pages/index"
	"github.com/diamondburned/tmplutil"
	"github.com/pkg/errors"
)

var dbPath string

func init() {
	flag.StringVar(&dbPath, "db", dbPath, "badgerdb path")
	flag.Parse()

	if dbPath == "" {
		log.Fatalln("missing -db flag.")
	}
}

func main() {
	r := http.NewServeMux()
	r.Handle("/static", http.StripPrefix("/static", frontend.MountStatic()))
	r.Handle("/", tmplutil.AlwaysFlush(http.HandlerFunc(root)))

	if err := cgi.Serve(r); err != nil {
		log.Fatalln("failed to serve:", err)
	}
}

var snapshotTimes = []index.SnapshotTime{
	{Time: "1 hour", Dura: time.Hour},
	{Time: "24 hours", Dura: 1 * 24 * time.Hour},
	{Time: "1 week", Dura: 7 * 24 * time.Hour},
}

const maxTime = 365 * 24 * time.Hour // max 1yr

func root(w http.ResponseWriter, r *http.Request) {
	// Default to rendering last 3 hours' data.
	duration := 3 * time.Hour

	if t := r.FormValue("t"); t != "" {
		d, err := time.ParseDuration(r.FormValue("t"))
		if err != nil {
			errpage.Render(w, errors.Wrap(err, "invalid value for t"))
			return
		}

		if d > maxTime {
			errpage.Render(w, fmt.Errorf("duration %v is over bound %v", d, maxTime))
			return
		}

		duration = d
	}

	index.Render(w, dbPath, duration)
}
