package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"git.unix.lgbt/diamondburned/sysmet"
	"github.com/pkg/errors"
)

func main() {
	var (
		dbPath string
		gcDays int64
	)

	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(),
			"Usage:")
		fmt.Fprintln(flag.CommandLine.Output(),
			"  "+filepath.Base(os.Args[0]), "-db path [flags...]", "[gc]")
		fmt.Fprintln(flag.CommandLine.Output(),
			"")
		fmt.Fprintln(flag.CommandLine.Output(),
			"Flags:")
		flag.PrintDefaults()
	}

	flag.Int64Var(&gcDays, "gc", gcDays, "number of days ago to delete, 0 to not delete")
	flag.StringVar(&dbPath, "db", dbPath, "badgerdb path")
	flag.Parse()

	if dbPath == "" {
		log.Fatalln("missing -db flag; refer to -h.")
	}

	var err error

	if gcDays > 0 {
		err = gc(dbPath, gcDays)
	} else {
		err = update(dbPath)
	}

	if err != nil {
		log.Fatalln("unexpected error:", err)
	}
}

func update(dbPath string) error {
	s, err := sysmet.PrepareMetrics()
	if err != nil {
		return errors.Wrap(err, "failed to take snapshot")
	}

	d, err := sysmet.Open(dbPath, true)
	if err != nil {
		return errors.Wrap(err, "failed to open database")
	}
	defer d.Close()

	if err := d.Update(s); err != nil {
		return errors.Wrap(err, "failed to update")
	}

	if err := d.Close(); err != nil {
		return errors.Wrap(err, "failed to close")
	}

	return nil
}

func gc(dbPath string, gcDays int64) error {
	d, err := sysmet.Open(dbPath, true)
	if err != nil {
		return errors.Wrap(err, "failed to open database")
	}
	defer d.Close()

	if err := d.GC(time.Duration(gcDays) * 24 * time.Hour); err != nil {
		return errors.Wrap(err, "failed to GC")
	}

	if err := d.Close(); err != nil {
		return errors.Wrap(err, "failed to close")
	}

	return nil
}
