package timeseries

import (
	"database/sql"
	"fmt"
	cmp "github.com/google/go-cmp/cmp"
	"log"
	"testing"
	"time"
	"os"
)

const logFile = "TestLogFile"
const createLoglinesTableStmt = `
CREATE TABLE loglines (
  id integer primary key autoincrement,
  remote_host varchar(255),
  user varchar(255),
  authuser varchar(255),
  timestamp integer,
  request_method varchar(255),
  request_path varchar(255),
  response_status integer,
  response_bytes integer,
  log_file varchar(255)
)
`

type logLineRow struct {
	Id        int
	Ip        string
	User      string
	AuthUser  string
	Timestamp time.Time
	Method    string
	Path      string
	Status    uint16
	Bytes     int
	LogFile   string
}

func assertRowsEqual(t *testing.T, expected, actual logLineRow) {
	if !cmp.Equal(expected, actual) {
		t.Error(fmt.Sprintf("Expected: %v\nActual: %v\n", expected, actual))
	}
}

func loadDB() *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(createLoglinesTableStmt)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func parseTime(timeStr string) time.Time {
	time, err := time.Parse("02/Jan/2006:15:04:05 -0700", timeStr)
	if err != nil {
		log.Fatal(err)
	}
	return time
}

func TestRecord(t *testing.T) {
	db := loadDB()
	defer db.Close()
	ts := LogTimeSeries{db, logFile}

	lineOne := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:16:00:39 +0000"),
		"GET",
		"/report",
		200,
		123,
	}

	result, err := ts.Record(lineOne)
	if err != nil {
		t.Error(err)
	}

	rowsAffected, err := result.RowsAffected()
	if rowsAffected != 1 {
		t.Error(fmt.Sprintf("Expected 1 row affected but got %d", rowsAffected))
	}

	rows, err := db.Query("SELECT * FROM loglines")
	if err != nil {
		t.Error(err)
	}
	defer rows.Close()

	expected := logLineRow{
		1,
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:16:00:39 +0000"),
		"GET",
		"/report",
		200,
		123,
		logFile,
	}
	rows.Next()
	actual := logLineRow{}
	err = rows.Scan(&actual.Id, &actual.Ip, &actual.User, &actual.AuthUser, &actual.Timestamp,
		&actual.Method, &actual.Path, &actual.Status, &actual.Bytes, &actual.LogFile)
	if err != nil {
		t.Error(err)
	}
	assertRowsEqual(t, expected, actual)

	lineTwo := LogLine{
		"127.0.0.1",
		"-",
		"jill",
		parseTime("09/May/2018:16:00:41 +0000"),
		"GET",
		"/api/user",
		200,
		234,
	}
	result,err = ts.Record(lineTwo)
}

func TestMostCommonStatus(t *testing.T) {

}
