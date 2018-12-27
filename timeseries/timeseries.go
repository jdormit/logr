/*
Package timeseries implements a time series datastore to store and query log lines.

It stores the log lines in a SQL database. It does actually construct the database -
instead, the database must be set up and passed into LogTimeSeries instances. The
CreateLogLinesTableStmt is provided to ensure that callers can construct a database
with the correct schema.
*/
package timeseries

import (
	"database/sql"
	"strings"
	"time"
)

// CreateLogLinesTableStmt is the SQL statement to create the loglines table.
// It should be used to initialize any database passed into a LogTimeSeries
const CreateLogLinesTableStmt = `
CREATE TABLE IF NOT EXISTS loglines (
  id integer primary key autoincrement,
  remote_host varchar(255),
  user varchar(255),
  authuser varchar(255),
  timestamp integer,
  request_method varchar(255),
  request_section varchar(255),
  request_path varchar(255),
  response_status integer,
  response_bytes integer,
  log_file varchar(255)
)
`

// LogLine is the data structure representing a single line in a server log
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

// The LogTimeSeries struct is used to record and query log lines
type LogTimeSeries struct {
	DB      *sql.DB
	LogFile string
}

// The extractSection function returns the part of the input string after the
// first '/', e.g. extractSection("/api/user") returns "api".
func extractSection(path string) string {
	split := strings.Split(path, "/")
	if len(split) > 1 {
		return split[1]
	} else {
		return split[0]
	}
}

// Record persists a LogLine to the time series datastore
func (ts *LogTimeSeries) Record(logLine LogLine) (result sql.Result, err error) {
	result, err = ts.DB.Exec("INSERT INTO loglines "+
		"(remote_host, user, authuser, timestamp, request_method, "+
		"request_section, request_path, response_status, "+
		"response_bytes, log_file) "+
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
		logLine.Host, logLine.User, logLine.AuthUser, logLine.Timestamp.Unix(),
		logLine.Method, extractSection(logLine.Path), logLine.Path,
		logLine.Status, logLine.ResponseBytes, ts.LogFile)
	return
}

// MostCommonStatus returns the most common response status in all the LogLines
// recorded between `start` and `end`.
func (ts *LogTimeSeries) MostCommonStatus(start time.Time, end time.Time) (status uint16, err error) {
	row := ts.DB.QueryRow("SELECT response_status FROM loglines "+
		"WHERE log_file LIKE $1 AND timestamp BETWEEN $2 AND $3 "+
		"GROUP BY response_status "+
		"ORDER BY count(*) DESC "+
		"LIMIT 1", ts.LogFile, start.Unix(), end.Unix())
	err = row.Scan(&status)
	return
}

type Count struct {
	Label string
	Count int
}

// GetStatusCounts returns a slice of (status code, count) tuples sorted by count
// (descending) from log lines recorded between `start` and `end`
func (ts *LogTimeSeries) GetStatusCounts(start time.Time, end time.Time) (counts []Count, err error) {
	rows, err := ts.DB.Query("SELECT response_status, count(*) FROM loglines "+
		"WHERE log_file LIKE $1 AND timestamp BETWEEN $2 AND $3 "+
		"GROUP BY response_status "+
		"ORDER BY count(*) DESC", ts.LogFile, start.Unix(), end.Unix())
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		count := Count{}
		rows.Scan(&count.Label, &count.Count)
		counts = append(counts, count)
	}
	return
}

// MostRequested Section returns the most common path section in all the LogLines
// recorded between `start` and `end`. A path section is the part of the path
// after the first '/', e.g. the section for "/api/user" is "api"
func (ts *LogTimeSeries) MostRequestedSection(start time.Time, end time.Time) (section string, err error) {
	row := ts.DB.QueryRow("SELECT request_section FROM loglines "+
		"WHERE log_file LIKE $1 AND timestamp BETWEEN $2 AND $3 "+
		"GROUP BY request_section "+
		"ORDER BY count(*) DESC "+
		"LIMIT 1", ts.LogFile, start.Unix(), end.Unix())
	err = row.Scan(&section)
	return
}

// GetSectionCounts returns a slice of (section, count) tuples sorted by count
// (descending) from log lines recorded between `start` and `end`
func (ts *LogTimeSeries) GetSectionCounts(start time.Time, end time.Time) (counts []Count, err error) {
	rows, err := ts.DB.Query("SELECT request_section, count(*) FROM loglines "+
		"WHERE log_file LIKE $1 AND timestamp BETWEEN $2 AND $3 "+
		"GROUP BY request_section "+
		"ORDER BY count(*) DESC", ts.LogFile, start.Unix(), end.Unix())
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		count := Count{}
		rows.Scan(&count.Label, &count.Count)
		counts = append(counts, count)
	}
	return
}
