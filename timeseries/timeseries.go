package timeseries

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"time"
	"strings"
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
  request_section varchar(255),
  request_path varchar(255),
  response_status integer,
  response_bytes integer,
  log_file varchar(255)
)
*/
// dbFile should ~/.local/share/logr/db.sqlite

func extractSection(path string) string {
	split := strings.Split(path, "/")
	if len(split) > 1 {
		return split[1]
	} else {
		return split[0]
	}
}

func (ts *LogTimeSeries) Record(logLine LogLine) (result sql.Result, err error) {
	result, err = ts.db.Exec("INSERT INTO loglines "+
		"(remote_host, user, authuser, timestamp, request_method, "+
		"request_section, request_path, response_status, "+
		"response_bytes, log_file) "+
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
		logLine.Host, logLine.User, logLine.AuthUser, logLine.Timestamp.Unix(),
		logLine.Method, extractSection(logLine.Path), logLine.Path,
		logLine.Status, logLine.ResponseBytes, ts.logFile)
	return
}

// func RetrieveRange(start time.Time, end time.Time) []LogLine {
	
// }

func (ts *LogTimeSeries) MostCommonStatus(start time.Time, end time.Time) (status uint16, err error) {
	row := ts.db.QueryRow("SELECT response_status FROM loglines "+
		"WHERE log_file LIKE $1 AND timestamp BETWEEN $2 AND $3 "+
		"GROUP BY response_status "+
		"ORDER BY count(*) DESC "+
		"LIMIT 1", ts.logFile, start.Unix(), end.Unix())
	err = row.Scan(&status)
	return
}

type statusCount struct {
	Status uint16
	Count int
}

// Returns a slice of (status code, count) tuples sorted by count descending
// from log lines between `start` and `end`
func (ts *LogTimeSeries) GetStatusCounts(start time.Time, end time.Time) (counts []statusCount, err error) {
	rows, err := ts.db.Query("SELECT response_status, count(*) FROM loglines "+
		"WHERE log_file LIKE $1 AND timestamp BETWEEN $2 AND $3 "+
		"GROUP BY response_status "+
		"ORDER BY count(*) DESC", ts.logFile, start.Unix(), end.Unix())
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		count := statusCount{}
		rows.Scan(&count.Status, &count.Count)
		counts = append(counts, count)
	}
	return
}

func (ts *LogTimeSeries) MostRequestedSection(start time.Time, end time.Time) (section string, err error) {
	row := ts.db.QueryRow("SELECT request_section FROM loglines "+
		"WHERE log_file LIKE $1 AND timestamp BETWEEN $2 AND $3 "+
		"GROUP BY request_section "+
		"ORDER BY count(*) DESC "+
		"LIMIT 1", ts.logFile, start.Unix(), end.Unix())
	err = row.Scan(&section)
	return
}
