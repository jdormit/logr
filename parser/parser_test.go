package parser

import (
	"github.com/jdormit/logr/timeseries"
	"log"
	"testing"
	"time"
	"github.com/google/go-cmp/cmp"
)

func parseTime(timeStr string) time.Time {
	time, err := time.Parse("02/Jan/2006:15:04:05 -0700", timeStr)
	if err != nil {
		log.Fatal(err)
	}
	return time
}

func TestParseLogLine(t *testing.T) {
	testCases := []struct {
		inputLine      string
		expectedOutput timeseries.LogLine
		expectedError  error
	}{
		{
			inputLine: `127.0.0.1 - james [09/May/2018:16:00:39 +0000] "GET /report HTTP/1.0" 200 123`,
			expectedOutput: timeseries.LogLine{
				"127.0.0.1",
				"-",
				"james",
				parseTime("09/May/2018:16:00:39 +0000"),
				"GET",
				"/report",
				200,
				123,
			},
		},
		{
			inputLine: `Not a real log line`,
			expectedError: ParseError,
		},
		{
			inputLine: ``,
			expectedError: ParseError,
		},
		{
			inputLine: `127.0.0.1 - james [09/May/2018:16:00:39 +0000] "GET /report HTTP/1.0" 200 123 foo bar baz some more stuff [with brackets]`,
			expectedOutput: timeseries.LogLine{
				"127.0.0.1",
				"-",
				"james",
				parseTime("09/May/2018:16:00:39 +0000"),
				"GET",
				"/report",
				200,
				123,
			},
		},
	}
	for caseIdx, testCase := range testCases {
		logLine, err := ParseLogLine(testCase.inputLine)
		if testCase.expectedError != nil {
			if err != testCase.expectedError {
				t.Errorf("Error on case %d.\nExpected: %#v\nActual: %#v",
					caseIdx, testCase.expectedError, err)
			}
			continue
		}
		if !cmp.Equal(testCase.expectedOutput, logLine) {
			t.Errorf("Error on case %d.\nExpected: %#v\nActual: %#v",
				caseIdx, testCase.expectedOutput, logLine)
		}
	}
}
