package main

import (
	"flag"
	"log"
	"net/http"
	"net/http/cgi"

	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/pages/index"
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
	r.Handle("/", AlwaysFlush(http.HandlerFunc(root)))

	if err := cgi.Serve(r); err != nil {
		log.Fatalln("failed to serve:", err)
	}
}

func root(w http.ResponseWriter, r *http.Request) {
	if err := index.Render(w, dbPath); err != nil {
		log.Println("failed to render:", err)
	}
}
