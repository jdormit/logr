package timeseries

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"time"
)

type LogLine struct {
	Host          string
	User          string
	AuthUser      string
	Timestamp     time.Time
	Method        string
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
  request_method varchar(255),
  request_path varchar(255),
  response_status integer,
  response_bytes integer,
  log_file varchar(255)
)
*/
// dbFile should ~/.local/share/logr/db.sqlite

func (ts *LogTimeSeries) Record(logLine LogLine) (result sql.Result, err error) {
	result, err = ts.db.Exec("INSERT INTO loglines "+
		"(remote_host, user, authuser, timestamp, request_path, response_status, response_bytes, log_file) "+
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		logLine.Host, logLine.User, logLine.AuthUser, logLine.Timestamp,
		logLine.Method, logLine.Path, logLine.Status, logLine.ResponseBytes, ts.logFile)
	return
}

// func RetrieveRange(start time.Time, end time.Time) []LogLine {
	
// }

func (ts *LogTimeSeries) MostCommonStatus(start time.Time, end time.Time) (status uint16, err error) {
	rows, err := ts.db.Query("SELECT response_status, count(*) as count FROM loglines "+
		"WHERE log_file LIKE $1 AND timestamp BETWEEN $2 AND $3 "+
		"GROUP BY response_status"+
		"ORDER BY count DESC"+
		"LIMIT 1", ts.logFile, start.Unix(), end.Unix())
	defer rows.Close()
	rows.Next()
	rows.Scan(&status)
	return
}

func MostRequestedSection(start time.Time, end time.Time) string {
	return "TODO"
}
