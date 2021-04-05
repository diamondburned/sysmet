package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/cgi"
	"strings"
	"time"

	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend"
	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend/pages/errpage"
	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend/pages/index"
	"github.com/diamondburned/tmplutil"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"maze.io/x/duration"
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
	r := chi.NewRouter()
	r.Mount("/static", http.StripPrefix("/static", frontend.MountStatic()))
	r.Group(func(r chi.Router) {
		r.Use(tmplutil.AlwaysFlush)
		r.Use(middleware.NoCache)
		r.Use(middleware.Compress(5))

		// TODO: remove * once FCGI works; blame NGINX.
		r.Get("/*", root)
	})

	if err := cgi.Serve(r); err != nil {
		log.Fatalln("failed to serve:", err)
	}
}

const maxTime = 365 * 24 * time.Hour // max 1yr

type jsonError struct {
	Error string
}

func root(w http.ResponseWriter, r *http.Request) {
	dura, err := parseDuration(r)

	for _, accept := range strings.Split(r.Header.Get("Accept"), ",") {
		switch accept {
		case "application/json":
			w.Header().Set("Content-Type", "application/json; charset=UTF-8")

			if err != nil {
				w.WriteHeader(400)
				json.NewEncoder(w).Encode(jsonError{Error: err.Error()})
				return
			}

			frontend.WriteJSON(w, dbPath, dura)
			return

		case "text/html":
			fallthrough
		default:
			w.Header().Set("Content-Type", "text/html; charset=UTF-8")

			if err != nil {
				errpage.Respond(w, 400, err)
				return
			}

			index.Render(w, r, dbPath, dura)
			return
		}
	}
}

func parseDuration(r *http.Request) (time.Duration, error) {
	// Default to rendering last 3 hours' data.
	dura := 3 * time.Hour

	if t := r.FormValue("t"); t != "" {
		d, err := duration.ParseDuration(t)
		if err != nil {
			return 0, err
		}

		dura = time.Duration(d)

		if dura < 0 || dura > maxTime {
			return 0, fmt.Errorf("duration %v is over bound %v", d, maxTime)
		}
	}

	return dura, nil
}
