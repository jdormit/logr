package timeseries

import (
	"database/sql"
	"github.com/google/go-cmp/cmp"
	"log"
	"testing"
	"time"
	_ "github.com/mattn/go-sqlite3"
)

const logFile = "TestLogFile"

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
		t.Errorf("Expected: %#v\nActual: %#v\n", expected, actual)
	}
}

func loadDB() (db *sql.DB, err error) {
	db, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		return
	}
	_, err = db.Exec(CreateLogLinesTableStmt)
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
				t.Errorf("Expected 1 row affected but got %d", rowsAffected)
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
				t.Errorf("Error on case %d.\nExpected: %#v\nActual: %#v\n",
					caseIdx, testCase.expectedRow, actual)
			}
		}()
	}
}

func TestMostCommonStatus(t *testing.T) {
	testCases := []struct{
		inputLines []LogLine
		expectedStatus uint16
		start time.Time
		end time.Time
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
					"james",
					parseTime("09/May/2018:17:00:39 +0000"),
					"GET",
					"/report",
					500,
					123,
				},
				LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:18:00:39 +0000"),
					"GET",
					"/report",
					200,
					123,
				},
			},
			200,
			parseTime("09/May/2018:15:00:39 +0000"),
			parseTime("09/May/2018:19:00:39 +0000"),
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
					"james",
					parseTime("09/May/2018:17:00:39 +0000"),
					"GET",
					"/report",
					500,
					123,
				},
				LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:18:00:39 +0000"),
					"GET",
					"/report",
					200,
					123,
				},
				LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:19:00:39 +0000"),
					"GET",
					"/report",
					500,
					123,
				},
			},
			200,
			parseTime("09/May/2018:15:00:39 +0000"),
			parseTime("09/May/2018:20:00:39 +0000"),
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
					"james",
					parseTime("09/May/2018:17:00:39 +0000"),
					"GET",
					"/report",
					500,
					123,
				},
				LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:18:00:39 +0000"),
					"GET",
					"/report",
					200,
					123,
				},
				LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:19:00:39 +0000"),
					"GET",
					"/report",
					500,
					123,
				},
			},
			500,
			parseTime("09/May/2018:17:00:00 +0000"),
			parseTime("09/May/2018:20:00:39 +0000"),
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
					"james",
					parseTime("09/May/2018:17:00:39 +0000"),
					"GET",
					"/report",
					500,
					123,
				},
				LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:18:00:39 +0000"),
					"GET",
					"/report",
					200,
					123,
				},
				LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:19:00:39 +0000"),
					"GET",
					"/report",
					500,
					123,
				},
			},
			200,
			parseTime("09/May/2018:15:00:00 +0000"),
			parseTime("09/May/2018:19:00:00 +0000"),
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
			for _, logLine := range testCase.inputLines {
				_, err := ts.Record(logLine)
				if err != nil {
					t.Error(err)
				}
			}
			actualStatus, err := ts.MostCommonStatus(testCase.start, testCase.end)
			if err != nil {
				t.Error(err)
			}
			if actualStatus != testCase.expectedStatus {
				t.Errorf("Error on case %d.\nExpected: %#v\nActual:%#v\n",
					caseIdx, testCase.expectedStatus, actualStatus)
			}
		}()
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

func TestGetStatusCounts(t *testing.T) {
	testCases := []struct{
		inputLines []LogLine
		expectedCounts []statusCount
		start time.Time
		end time.Time
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
					"james",
					parseTime("09/May/2018:17:00:39 +0000"),
					"GET",
					"/report",
					500,
					123,
				},
				LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:18:00:39 +0000"),
					"GET",
					"/report",
					200,
					123,
				},
			},
			[]statusCount{
				{200, 2},
				{500, 1},
			},
			parseTime("09/May/2018:15:00:39 +0000"),
			parseTime("09/May/2018:19:00:39 +0000"),
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
					"james",
					parseTime("09/May/2018:17:00:39 +0000"),
					"GET",
					"/report",
					500,
					123,
				},
				LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:18:00:39 +0000"),
					"GET",
					"/report",
					200,
					123,
				},
			},
			[]statusCount{
				{200, 1},
				{500, 1},
			},
			parseTime("09/May/2018:17:00:00 +0000"),
			parseTime("09/May/2018:19:00:00 +0000"),
		},
		{
			[]LogLine{},
			nil,
			parseTime("09/May/2018:17:00:00 +0000"),
			parseTime("09/May/2018:19:00:00 +0000"),
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
			for _, logLine := range testCase.inputLines {
				_, err = ts.Record(logLine)
				if err != nil {
					t.Error(err)
				}
			}
			actualCounts, err := ts.GetStatusCounts(testCase.start, testCase.end)
			if err != nil {
				t.Error(err)
			}
			if !cmp.Equal(testCase.expectedCounts, actualCounts) {
				t.Errorf("Error on case %d.\nExpected: %#v\nActual: %#v\n",
					caseIdx, testCase.expectedCounts, actualCounts)
			}
		}()
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

func TestMostRequestSectionEmptyDb(t *testing.T) {
	db, err := loadDB()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	ts := LogTimeSeries{db, logFile}
	start := parseTime("09/May/2018:15:00:00 +0000")
	end := parseTime("09/May/2018:16:00:40 +0000")
	_, err = ts.MostRequestedSection(start, end)
	if err != sql.ErrNoRows {
		t.Fail()
	}
}

func TestGetSectionCounts(t *testing.T) {
	testCases := []struct{
		inputLines []LogLine
		expectedCounts []sectionCount
		start time.Time
		end time.Time
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
					"james",
					parseTime("09/May/2018:17:00:39 +0000"),
					"GET",
					"/api/user",
					500,
					123,
				},
				LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:18:00:39 +0000"),
					"GET",
					"/report",
					200,
					123,
				},
			},
			[]sectionCount {
				{"report", 2},
				{"api", 1},
			},
			parseTime("09/May/2018:15:00:39 +0000"),
			parseTime("09/May/2018:19:00:39 +0000"),
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
					"james",
					parseTime("09/May/2018:17:00:39 +0000"),
					"GET",
					"/api/user",
					500,
					123,
				},
				LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:18:00:39 +0000"),
					"GET",
					"/report",
					200,
					123,
				},
			},
			[]sectionCount {
				{"api", 1},
				{"report", 1},
			},
			parseTime("09/May/2018:17:00:00 +0000"),
			parseTime("09/May/2018:19:00:00 +0000"),
		},
		{
			[]LogLine{},
			nil,
			parseTime("09/May/2018:17:00:00 +0000"),
			parseTime("09/May/2018:19:00:00 +0000"),
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
			for _, logLine := range testCase.inputLines {
				_, err = ts.Record(logLine)
				if err != nil {
					t.Error(err)
				}
			}
			actualCounts, err := ts.GetSectionCounts(testCase.start, testCase.end)
			if err != nil {
				t.Error(err)
			}
			if !cmp.Equal(testCase.expectedCounts, actualCounts) {
				t.Errorf("Error on case %d.\nExpected: %#v\nActual: %#v\n",
					caseIdx, testCase.expectedCounts, actualCounts)
			}
		}()
	}
}
