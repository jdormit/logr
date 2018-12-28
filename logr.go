package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/gizak/termui"
	"github.com/jdormit/logr/offsets"
	"github.com/jdormit/logr/reader"
	"github.com/jdormit/logr/timeseries"
	"github.com/jdormit/logr/ui"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"os/signal"
	"path"
	"time"
)

const defaultTimescale = 5
const defaultGranularity = 10
const defaultAlertThreshold = 10.0
const defaultAlertInterval = 120

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
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Usage = usage

	defaultDebugLogPath := path.Join(os.Getenv("HOME"), ".local", "share", "logr", "logr.log")
	debugLogPath := flag.String("debugLogPath", defaultDebugLogPath, "The `path` to the file where logr will write debug logs")

	defaultDbPath := path.Join(os.Getenv("HOME"), ".local", "share", "logr", "logr.sqlite")
	dbPath := flag.String("dbPath", defaultDbPath, "The `path` to the SQLite database")

	alertThreshold := flag.Float64("alertThreshold", defaultAlertThreshold, "The average number of requests per second over the alerting interval that will trigger an alert")
	alertInterval := flag.Int("alertInterval", defaultAlertInterval, "The interval of time in seconds during which the number of requests per second must exceed the alert threshold to trigger an alert")
	timescale := flag.Int("timescale", defaultTimescale, "The size of the reporting time window in minutes")
	granularity := flag.Int("granularity", defaultGranularity, "The granularity of the traffic graph, i.e. the number of buckets into which traffic is divided.")

	flag.Parse()

	err := os.MkdirAll(path.Dir(*debugLogPath), 0755)
	if err != nil {
		log.Fatal(err)
	}
	err = os.Remove(*debugLogPath)
	if err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}
	debugLogFile, err := os.Create(*debugLogPath)
	if err != nil {
		log.Fatal(err)
	}
	defer debugLogFile.Close()
	log.SetOutput(debugLogFile)

	err = os.MkdirAll(path.Dir(*dbPath), 0755)
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
	logReader := reader.NewLogReader(&offsetPersister, logPath)

	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)

	updateTicker := time.NewTicker(time.Second).C

	logChan := make(chan timeseries.LogLine, 24)
	go logReader.TailLogFile(logChan)
	defer logReader.Terminate()

	logTimeSeries := timeseries.LogTimeSeries{db, logPath}

	err = termui.Init()
	if err != nil {
		log.Fatal(err)
	}
	defer termui.Close()

	uiState, err := ui.GetInitialUIState(&logTimeSeries, *timescale, *granularity,
		*alertThreshold, *alertInterval)
	if err != nil {
		log.Fatal(err)
	}
	ui.Render(uiState)

	uiEvents := termui.PollEvents()

	for {
		select {
		case <-interrupts:
			logReader.Terminate()
			return
		case e := <-uiEvents:
			switch e.ID {
			case "<C-c>":
				logReader.Terminate()
				return
			case "<Resize>":
				ui.Render(uiState)
			}
		case logLine := <-logChan:
			_, err = logTimeSeries.Record(logLine)
			if err != nil {
				log.Printf("Error writing log line to database: %v", err)
			}
		case <-updateTicker:
			uiState := ui.NextUIState(uiState, &logTimeSeries, time.Now())
			ui.Render(uiState)
		}
	}
}
