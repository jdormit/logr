package timeseries

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"time"
)

type LogLine struct {
	Host          string
	User          string
	AuthUser      string
	Timestamp     time.Time
	Path          string
	Status        uint16
	ResponseBytes int
}

type LogTimeSeries struct {
	db      *sql.DB
	logFile string
}

// TODO create the database and the loglines table
/* schema:
CREATE TABLE loglines (
  id integer auto_increment primary key,
  remote_host varchar(255),
  user varchar(255),
  timestamp integer,
  request_path varchar(255),
  response_status integer,
  response_bytes integer
)
*/
// dbFile should ~/.local/share/logr/db.sqlite

func (ts *LogTimeSeries) Record(logLine LogLine) {
	result, err := ts.db.Exec("INSERT INTO loglines "+
		"(remote_host, user, authuser, timestamp, request_path, response_status, response_bytes) "+
		"VALUES ($1, $2, $3, $4, $5, $6, $7)",
		logLine.Host, logLine.User, logLine.AuthUser, logLine.Timestamp,
		logLine.Path, logLine.Status, logLine.ResponseBytes)
}

func RetrieveRange(start time.Time, end time.Time) []LogLine {

}

func MostCommonStatus(start time.Time, end time.Time) uint16 {

}

func MostRequestedSection(start time.Time, end time.Time) string {

}
