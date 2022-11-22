package main

import (
	"flag"
	"log"
	"net/http/cgi"

	"git.unix.lgbt/diamondburned/sysmet/v3/cmd/sysmet-http/handler"
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
	if err := cgi.Serve(handler.New(dbPath)); err != nil {
		log.Fatalln("failed to serve:", err)
	}
}
