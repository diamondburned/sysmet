package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-http/handler"
)

var dbPath string

func init() {
	p := func(v ...interface{}) { fmt.Fprintln(flag.CommandLine.Output(), v...) }
	flag.Usage = func() {
		p("Usage:")
		p("  sysmet-http -db <badgerdb path> <http address>")
		p("")
		p("Flags:")
		flag.PrintDefaults()
	}

	flag.StringVar(&dbPath, "db", dbPath, "badgerdb path")
	flag.Parse()
}

func main() {
	if dbPath == "" {
		log.Fatalln("missing -db flag, see -h")
	}

	listen := flag.Arg(0)
	if listen == "" {
		log.Println("missing listen addr, see -h")
	}

	if err := http.ListenAndServe(listen, handler.New(dbPath)); err != nil {
		log.Fatalln("failed to serve:", err)
	}
}
