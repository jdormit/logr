package ui

import (
	"database/sql"
	"github.com/google/go-cmp/cmp"
	"github.com/jdormit/logr/timeseries"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"testing"
	"time"
)

const logFile = "logfile.log"

func loadDB() (db *sql.DB, err error) {
	db, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		return
	}
	_, err = db.Exec(timeseries.CreateLogLinesTableStmt)
	return
}

func parseTime(timeStr string) time.Time {
	time, err := time.Parse("02/Jan/2006:15:04:05 -0700", timeStr)
	if err != nil {
		log.Fatal(err)
	}
	return time
}

func TestNextUIState(t *testing.T) {
	testCases := []struct {
		initialState  *UIState
		inputLines    []timeseries.LogLine
		now           time.Time
		expectedState *UIState
	}{
		{
			&UIState{
				Timescale:      5,
				Begin:          parseTime("09/May/2018:18:00:00 +0000"),
				Granularity:    5,
				AlertThreshold: 1,
				AlertInterval:  1,
			},
			[]timeseries.LogLine{
				timeseries.LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:18:03:00 +0000"),
					"GET",
					"/report",
					200,
					123,
				},
			},
			parseTime("09/May/2018:18:03:01 +0000"),
			&UIState{
				Timescale:      5,
				Begin:          parseTime("09/May/2018:18:00:00 +0000"),
				Granularity:    5,
				AlertThreshold: 1,
				AlertInterval:  1,
				Alert:          false,
				SectionCounts: []timeseries.Count{
					timeseries.Count{"report", 1},
				},
				StatusCounts: []timeseries.Count{
					timeseries.Count{"200", 1},
				},
				Traffic: []int{0, 0, 0, 1, 0},
			},
		},
		{
			&UIState{
				Timescale:      5,
				Begin:          parseTime("09/May/2018:18:00:00 +0000"),
				Granularity:    5,
				AlertThreshold: 1,
				AlertInterval:  1,
			},
			[]timeseries.LogLine{
				timeseries.LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:18:03:00 +0000"),
					"GET",
					"/report",
					200,
					123,
				},
				timeseries.LogLine{
					"127.0.0.1",
					"-",
					"james",
					parseTime("09/May/2018:18:03:00 +0000"),
					"GET",
					"/report",
					200,
					123,
				},
			},
			parseTime("09/May/2018:18:03:01 +0000"),
			&UIState{
				Timescale:      5,
				Begin:          parseTime("09/May/2018:18:00:00 +0000"),
				Granularity:    5,
				AlertThreshold: 1,
				AlertInterval:  1,
				Alert:          true,
				SectionCounts: []timeseries.Count{
					timeseries.Count{"report", 2},
				},
				StatusCounts: []timeseries.Count{
					timeseries.Count{"200", 2},
				},
				Traffic: []int{0, 0, 0, 2, 0},
			},
		},
	}
	for caseIdx, testCase := range testCases {
		func() {
			db, err := loadDB()
			if err != nil {
				t.Error(err)
			}
			defer db.Close()
			ts := timeseries.LogTimeSeries{db, logFile}
			for _, inputLine := range testCase.inputLines {
				ts.Record(inputLine)
			}
			actual := NextUIState(testCase.initialState, &ts, testCase.now)
			if !cmp.Equal(testCase.expectedState, actual) {
				t.Errorf("Error on test case %d.\nExpected: %+v\nActual: %+v",
					caseIdx, testCase.expectedState, actual)
			}
		}()
	}
}
