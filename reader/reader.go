// Package reader provides a function to tail a log file and send updates across a channel
package reader

import (
	"github.com/jdormit/logr/timeseries"
	"github.com/jdormit/logr/offsets"
	"github.com/jdormit/logr/parser"
	"bufio"
	"os"
	"log"
)

// A logReader tails a log file. It should be instantiated via reader.NewLogReader().
type logReader struct {
	offsetPersister *offsets.OffsetPersister
	shouldTerminate bool
}

// NewLogReader returns a new logReader struct.
func NewLogReader(offsetPersister *offsets.OffsetPersister) logReader {
	return logReader{offsetPersister, false}
}

// TailLogFile reads lines from the end of a log file and sends them over `logChan`.
// It will loop forever until a call to logReader.Terminate().
func (lr *logReader) TailLogFile(filepath string, logChan chan<- timeseries.LogLine) {
	lr.shouldTerminate = false
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("Error opening log file: %v", err)
	}
	defer file.Close()

	latestOffset, err := lr.offsetPersister.GetOffset(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err = lr.offsetPersister.PersistOffset(filepath, latestOffset)
		if err != nil {
			log.Fatal(err)
		}
	}()

	scanner := bufio.NewScanner(file)

	// Skip to the latest offset
	for i := int64(0); i < latestOffset; i++ {
		scanner.Scan()
	}

	for !lr.shouldTerminate {
		if scanner.Scan() {
			latestOffset = latestOffset + 1
			logLineStr := scanner.Text()
			logLine, err := parser.ParseLogLine(logLineStr)
			if err == nil {
				logChan <- logLine
			}
		} else if scanner.Err() != nil {
			log.Printf("Fatal error scanning log file: %v\nTerminating\n",
				scanner.Err())
			lr.Terminate()
		}
	}
}

func (lr *logReader) Terminate() {
	lr.shouldTerminate = true
}
