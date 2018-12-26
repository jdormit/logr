// Package parser provides a function to parse a log line into the LogLine data structure
package parser

import (
	"errors"
	"github.com/jdormit/logr/timeseries"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// A ParseError is returned when a log line cannot be parsed
var ParseError = errors.New("Unable to parse log line")

/*
Splits an input log line string into an array of tokens. Splits on whitespace except
in the case of the date and the request, which get returned as one token even though
they are made up of multiple words.

For example:
    splitLogLine("127.0.0.1 - james [09/May/2018:16:00:39 +0000] \"GET /report HTTP/1.0\" 200 123"

returns:
    []string{"127.0.0.1" "-" "james" "09/May/2018:16:00:39 +0000" "GET /report HTTP/1.0" "200" "123"}
*/
func splitLogLine(line string) (tokens []string, err error) {
	re, err := regexp.Compile(`^(.*)\[(.*)\] "(.*)" (.*)`)
	if err != nil {
		return
	}
	matches := re.FindStringSubmatch(line)
	if matches == nil {
		return nil, ParseError
	}
	for _, token := range strings.Split(matches[1], " ") {
		trimmed := strings.TrimSpace(token)
		if trimmed != "" {
			tokens = append(tokens, trimmed)
		}
	}
	tokens = append(tokens, strings.TrimSpace(matches[2]))
	tokens = append(tokens, strings.TrimSpace(matches[3]))
	for _, token := range strings.Split(matches[4], " ") {
		trimmed := strings.TrimSpace(token)
		if trimmed != "" {
			tokens = append(tokens, trimmed)
		}
	}
	return
}

// ParseLogLine parses a log line string into the LogLine data structure.
// It will return a ParseError if the line is not a valid log line.
func ParseLogLine(line string) (logLine timeseries.LogLine, err error) {
	tokens, err := splitLogLine(line)
	if err != nil {
		return
	}
	logLine = timeseries.LogLine{}
	for i, token := range tokens {
		switch i {
		case 0:
			logLine.Host = token
		case 1:
			logLine.User = token
		case 2:
			logLine.AuthUser = token
		case 3:
			timestamp, err := time.Parse("02/Jan/2006:15:04:05 -0700", token)
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
				logLine.ResponseBytes = responseBytes
			}
		default:
			break
		}
	}
	return
}
