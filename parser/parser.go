// Package parser provides a function to parse a log line into the LogLine data structure
package parser

import (
	"github.com/jdormit/logr/timeseries"
	"log"
	"strconv"
	"strings"
	"time"
)

/*
Splits an input log line string into an array of tokens. Splitting occurs
on whitespace, unless an expression is surrounded by square brackets ('[foo bar]')
or quotes ('"foo bar"').

For example:
    splitLogLine("127.0.0.1 - james [09/May/2018:16:00:39 +0000] \"GET /report HTTP/1.0\" 200 123"

returns:
    []string{"127.0.0.1" "-" "james" "09/May/2018:16:00:39 +0000" "GET /report HTTP/1.0" "200" "123"}
*/
func splitLogLine(line string) []string {

}

func ParseLogLine(line string) timeseries.LogLine {
	tokens := splitLogLine(line)
	logLine := timeseries.LogLine{}
	for i, token := range tokens {
		switch i {
		case 0:
			logLine.Host = token
		case 1:
			logLine.User = token
		case 2:
			logLine.AuthUser = token
		case 3:
			timestamp, err = time.Parse("02/Jan/2006:15:04:05 -0700", token)
			if err != nil {
				log.Printf("Unable to parse timestamp: %v", err)
			} else {
				logLine.Timestamp = timestamp
			}
		case 4:
			splitToken := strings.Split(token, " ")
			if len(splitToken) < 2 {
				log.Printf("Unable to parse method and path from %s", token)
			} else {
				logLine.Method = splitToken[0]
				logLine.Path = splitToken[1]
			}
		case 5:
			status, err := strconv.Atoi(token)
			if err != nil {
				log.Printf("Unable to parse response status from %s", token)
			} else {
				logLine.Status = uint16(status)
			}
		case 6:
			responseBytes, err := strconv.Atoi(token)
			if err != nil {
				log.Printf("Unable to parse response size from %s", token)
			} else {
				logLine.ResponseBytes = status
			}
		default:
			break
		}
	}
	return logLine
}
