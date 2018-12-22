package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"log"
)

var defaultLogPath = path.Join(os.TempDir(), "access.log")

func usage() {
	fmt.Printf(`A small utility to monitor a server log file

USAGE:
  %s [OPTIONS] [log_file_path]

ARGS:
  log_file_path
        The path to the log file to monitor (default %s)

OPTIONS:
  -h, -help
        Display this message and exit
`, os.Args[0], defaultLogPath)
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage

	defaultDbPath := path.Join(os.Getenv("HOME"), ".local", "share", "logr", "db.sqlite")
	dbPath := flag.String("dbPath", defaultDbPath, "The `path` to the SQLite database")

	flag.Parse()

	err := os.MkdirAll(path.Dir(*dbPath), 0755)
	if err != nil {
		log.Fatal(err)
	}

	var logPath string
	if flag.Arg(0) != "" {
		logPath = flag.Arg(0)
	} else {
		logPath = defaultLogPath
	}

	fmt.Printf("logPath is %v, dbPath is %v\n", logPath, *dbPath)
}
