package timeseries

import (
	"database/sql"
	"fmt"
	cmp "github.com/google/go-cmp/cmp"
	"log"
	"testing"
	"time"
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
  request_section varchar(255),
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
	Timestamp int64
	Method    string
	Section   string
	Path      string
	Status    uint16
	Bytes     int
	LogFile   string
}

func assertRowsEqual(t *testing.T, expected, actual logLineRow) {
	if !cmp.Equal(expected, actual) {
		t.Error(fmt.Sprintf("Expected: %#v\nActual: %#v\n", expected, actual))
	}
}

func loadDB() (db *sql.DB, err error) {
	db, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		return
	}
	_, err = db.Exec(createLoglinesTableStmt)
	if err != nil {
		return
	}
	return
}

func parseTime(timeStr string) time.Time {
	time, err := time.Parse("02/Jan/2006:15:04:05 -0700", timeStr)
	if err != nil {
		log.Fatal(err)
	}
	return time
}

func TestExtractSection(t *testing.T) {
	testCases := []struct {
		path            string
		expectedSection string
	}{
		{"/report", "report"},
		{"/api/user", "api"},
		{"", ""},
		{"noslash", "noslash"},
		{"//oops", ""},
	}
	for _, testCase := range testCases {
		actual := extractSection(testCase.path)
		if actual != testCase.expectedSection {
			t.Errorf("Expected: %#v\nActual: %#v\n", testCase.expectedSection, actual)
		}
	}
}

func TestRecord(t *testing.T)  {
	var emptyTimestamp time.Time
	testCases := []struct{
		inputRow LogLine
		expectedRow logLineRow
	}{
		{
			LogLine{
				"127.0.0.1",
				"-",
				"james",
				parseTime("09/May/2018:16:00:39 +0000"),
				"GET",
				"/report",
				200,
				123,
			},
			logLineRow{
				1,
				"127.0.0.1",
				"-",
				"james",
				parseTime("09/May/2018:16:00:39 +0000").Unix(),
				"GET",
				"report",
				"/report",
				200,
				123,
				logFile,
			},
		},
		{
			LogLine{},
			logLineRow{Id: 1, LogFile: logFile, Timestamp: emptyTimestamp.Unix()},
		},
	}
	for caseIdx, testCase := range testCases {
		func() {
			db, err := loadDB()
			if err != nil {
				t.Error(err)
			}
			defer db.Close()
			ts := LogTimeSeries{db, logFile}
			result, err := ts.Record(testCase.inputRow)
			if err != nil {
				t.Error(err)
			}

			rowsAffected, err := result.RowsAffected()
			if rowsAffected != 1 {
				t.Error(fmt.Sprintf("Expected 1 row affected but got %d",
					rowsAffected))
			}
			actual := logLineRow{}
			row := db.QueryRow("SELECT * FROM loglines")
			err = row.Scan(&actual.Id, &actual.Ip, &actual.User, &actual.AuthUser,
				&actual.Timestamp, &actual.Method, &actual.Section, &actual.Path,
				&actual.Status, &actual.Bytes, &actual.LogFile)
			if err != nil {
				t.Error(err)
			}
			if !cmp.Equal(testCase.expectedRow, actual) {
				t.Error(fmt.Sprintf("Error on case %d.\nExpected: %#v\nActual: %#v\n", caseIdx, testCase.expectedRow, actual))
			}
		}()
	}
}

func TestMostCommonStatus(t *testing.T) {
	db, err := loadDB()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	ts := LogTimeSeries{db, logFile}
	logLine1 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:16:00:39 +0000"),
		"GET",
		"/report",
		200,
		123,
	}
	logLine2 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:17:00:39 +0000"),
		"GET",
		"/report",
		500,
		123,
	}
	logLine3 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:18:00:39 +0000"),
		"GET",
		"/report",
		200,
		123,
	}
	_, err = ts.Record(logLine1)
	if err != nil {
		t.Error(err)
	}
	_, err = ts.Record(logLine2)
	if err != nil {
		t.Error(err)
	}
	_, err = ts.Record(logLine3)
	if err != nil {
		t.Error(err)
	}
	start := parseTime("09/May/2018:15:00:39 +0000")
	end := parseTime("09/May/2018:19:00:39 +0000")
	mostCommon, err := ts.MostCommonStatus(start, end)
	if err != nil {
		t.Error(err)
	}
	if mostCommon != 200 {
		t.Errorf("Expected 200, got %v", mostCommon)
	}
}

func TestItHandlesMostCommonStatusForEmptyDb(t *testing.T) {
	db, err := loadDB()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	ts := LogTimeSeries{db, logFile}
	start := parseTime("09/May/2018:15:00:39 +0000")
	end := parseTime("09/May/2018:19:00:39 +0000")
	_, err = ts.MostCommonStatus(start, end)
	if err != sql.ErrNoRows {
		t.Fail()
	}
}

func TestMostCommonStatusEqualCounts(t *testing.T) {
	db, err := loadDB()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	ts := LogTimeSeries{db, logFile}
	logLine1 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:16:00:39 +0000"),
		"GET",
		"/report",
		200,
		123,
	}
	logLine2 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:17:00:39 +0000"),
		"GET",
		"/report",
		500,
		123,
	}
	logLine3 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:18:00:39 +0000"),
		"GET",
		"/report",
		200,
		123,
	}
	logLine4 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:19:00:39 +0000"),
		"GET",
		"/report",
		500,
		123,
	}
	_, err = ts.Record(logLine1)
	if err != nil {
		t.Error(err)
	}
	_, err = ts.Record(logLine2)
	if err != nil {
		t.Error(err)
	}
	_, err = ts.Record(logLine3)
	if err != nil {
		t.Error(err)
	}
	_, err = ts.Record(logLine4)
	if err != nil {
		t.Error(err)
	}
	start := parseTime("09/May/2018:15:00:39 +0000")
	end := parseTime("09/May/2018:20:00:39 +0000")
	mostCommon, err := ts.MostCommonStatus(start, end)
	if err != nil {
		t.Error(err)
	}
	// 200 went in first, so that should come out first
	if mostCommon != 200 {
		t.Errorf("Expected 200, got %v", mostCommon)
	}
}

func TestItIgnoresStatusesBeforeTimeRange(t *testing.T) {
	db, err := loadDB()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	ts := LogTimeSeries{db, logFile}
	logLine1 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:16:00:39 +0000"),
		"GET",
		"/report",
		200,
		123,
	}
	logLine2 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:17:00:39 +0000"),
		"GET",
		"/report",
		500,
		123,
	}
	logLine3 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:18:00:39 +0000"),
		"GET",
		"/report",
		200,
		123,
	}
	logLine4 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:19:00:39 +0000"),
		"GET",
		"/report",
		500,
		123,
	}
	_, err = ts.Record(logLine1)
	if err != nil {
		t.Error(err)
	}
	_, err = ts.Record(logLine2)
	if err != nil {
		t.Error(err)
	}
	_, err = ts.Record(logLine3)
	if err != nil {
		t.Error(err)
	}
	_, err = ts.Record(logLine4)
	if err != nil {
		t.Error(err)
	}
	start := parseTime("09/May/2018:17:00:00 +0000")
	end := parseTime("09/May/2018:20:00:39 +0000")
	mostCommon, err := ts.MostCommonStatus(start, end)
	if err != nil {
		t.Error(err)
	}
	if mostCommon != 500 {
		t.Errorf("Expected 200, got %v", mostCommon)
	}
}

func TestItIgnoresStatusesAfterTimeRange(t *testing.T) {
	db, err := loadDB()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	ts := LogTimeSeries{db, logFile}
	logLine1 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:16:00:39 +0000"),
		"GET",
		"/report",
		200,
		123,
	}
	logLine2 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:17:00:39 +0000"),
		"GET",
		"/report",
		500,
		123,
	}
	logLine3 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:18:00:39 +0000"),
		"GET",
		"/report",
		200,
		123,
	}
	logLine4 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:19:00:39 +0000"),
		"GET",
		"/report",
		500,
		123,
	}
	_, err = ts.Record(logLine1)
	if err != nil {
		t.Error(err)
	}
	_, err = ts.Record(logLine2)
	if err != nil {
		t.Error(err)
	}
	_, err = ts.Record(logLine3)
	if err != nil {
		t.Error(err)
	}
	_, err = ts.Record(logLine4)
	if err != nil {
		t.Error(err)
	}
	start := parseTime("09/May/2018:15:00:00 +0000")
	end := parseTime("09/May/2018:19:00:00 +0000")
	mostCommon, err := ts.MostCommonStatus(start, end)
	if err != nil {
		t.Error(err)
	}
	if mostCommon != 200 {
		t.Errorf("Expected 200, got %v", mostCommon)
	}
}

func TestGetStatusCounts(t *testing.T) {
	db, err := loadDB()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	ts := LogTimeSeries{db, logFile}
	logLine1 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:16:00:39 +0000"),
		"GET",
		"/report",
		200,
		123,
	}
	logLine2 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:17:00:39 +0000"),
		"GET",
		"/report",
		500,
		123,
	}
	logLine3 := LogLine{
		"127.0.0.1",
		"-",
		"james",
		parseTime("09/May/2018:18:00:39 +0000"),
		"GET",
		"/report",
		200,
		123,
	}
	_, err = ts.Record(logLine1)
	if err != nil {
		t.Error(err)
	}
	_, err = ts.Record(logLine2)
	if err != nil {
		t.Error(err)
	}
	_, err = ts.Record(logLine3)
	if err != nil {
		t.Error(err)
	}
	start := parseTime("09/May/2018:15:00:39 +0000")
	end := parseTime("09/May/2018:19:00:39 +0000")
	counts, err := ts.GetStatusCounts(start, end)
	if err != nil {
		t.Error(err)
	}
	expected := []statusCount{
		statusCount{200, 2},
		statusCount{500, 1},
	}
	if !cmp.Equal(counts, expected) {
		t.Errorf("Expected: %#v\nActual: %#v\n", expected, counts)
	}
}

func TestMostRequestedSection(t *testing.T) {
	testCases := []struct {
		inputLog        []LogLine
		expectedSection string
		start           time.Time
		end             time.Time
	}{
		{
			[]LogLine{
				LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:16:00:39 +0000"),
					"GET",
					"/report",
					200,
					123,
				},
				LogLine{
					"127.0.0.1",
					"-",
					"jill",
					parseTime("09/May/2018:16:00:41 +0000"),
					"GET",
					"/api/user",
					200,
					234,
				},
				LogLine{
					"127.0.0.1",
					"-",
					"frank",
					parseTime("09/May/2018:16:00:42 +0000"),
					"POST",
					"/api/user",
					200,
					34,
				},
			},
			"api",
			parseTime("09/May/2018:15:00:00 +0000"),
			parseTime("09/May/2018:17:00:00 +0000"),
		},
		{
			[]LogLine{
				LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:16:00:39 +0000"),
					"GET",
					"/report",
					200,
					123,
				},
				LogLine{
					"127.0.0.1",
					"-",
					"jill",
					parseTime("09/May/2018:16:00:41 +0000"),
					"GET",
					"/api/user",
					200,
					234,
				},
				LogLine{
					"127.0.0.1",
					"-",
					"frank",
					parseTime("09/May/2018:16:00:42 +0000"),
					"POST",
					"/api/user",
					200,
					34,
				},
			},
			"report",
			parseTime("09/May/2018:15:00:00 +0000"),
			parseTime("09/May/2018:16:00:40 +0000"),
		},
	}
	for caseIdx, testCase := range testCases {
		func() {
			db, err := loadDB()
			if err != nil {
				t.Error(err)
			}
			defer db.Close()
			ts := LogTimeSeries{db, logFile}
			for i := range testCase.inputLog {
				ts.Record(testCase.inputLog[i])
			}
			section, err := ts.MostRequestedSection(testCase.start, testCase.end)
			if err != nil {
				t.Error(err)
			}
			if section != testCase.expectedSection {
				t.Errorf("Error on case %d.\nExpected: %#v\nActual: %#v\n",
					caseIdx, testCase.expectedSection, section)
			}
		}()
	}
}
