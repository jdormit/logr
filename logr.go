package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/jdormit/logr/offsets"
	"github.com/jdormit/logr/reader"
	"github.com/jdormit/logr/timeseries"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"os/signal"
	"path"
	"time"
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

func loadDB(dbPath string) (db *sql.DB, err error) {
	db, err = sql.Open("sqlite3", fmt.Sprintf("%s", dbPath))
	if err != nil {
		return
	}
	_, err = db.Exec(timeseries.CreateLogLinesTableStmt)
	if err != nil {
		return
	}
	_, err = db.Exec(offsets.CreateOffsetsTableStmt)
	return
}

func main() {
	flag.Usage = usage

	defaultDbPath := path.Join(os.Getenv("HOME"), ".local", "share", "logr", "logr.sqlite")
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

	db, err := loadDB(*dbPath)
	if err != nil {
		log.Fatal(err)
	}

	offsetPersister := offsets.OffsetPersister{db}
	logReader := reader.NewLogReader(&offsetPersister)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	updateTicker := time.NewTicker(time.Second).C

	logChan := make(chan timeseries.LogLine, 24)
	log.Printf("Tailing %s. C-c to quit.\n", logPath)
	go logReader.TailLogFile(logPath, logChan)

	logTimeSeries := timeseries.LogTimeSeries{db, logPath}

	for {
		select {
		case <-signalChan:
			logReader.Terminate()
			os.Exit(0)
		case logLine := <-logChan:
			_, err = logTimeSeries.Record(logLine)
			if err != nil {
				log.Printf("Error writing log line to database: %v", err)
			}
		case <-updateTicker:
			// Update UI
		}
	}
}
