// Package reader provides a function to tail a log file and send updates across a channel
package reader

import (
	"bufio"
	"github.com/jdormit/logr/offsets"
	"github.com/jdormit/logr/parser"
	"github.com/jdormit/logr/timeseries"
	"io"
	"log"
	"os"
)

// A logReader tails a log file. It should be instantiated via reader.NewLogReader().
type logReader struct {
	offsetPersister *offsets.OffsetPersister
	terminated      bool
	filepath        string
	offset          int64
}

// NewLogReader returns a new logReader struct.
func NewLogReader(offsetPersister *offsets.OffsetPersister, filename string) logReader {
	return logReader{offsetPersister, true, filename, 0}
}

// TailLogFile reads lines from the end of a log file and sends them over `logChan`.
// It will loop forever until a call to logReader.Terminate().
func (lr *logReader) TailLogFile(logChan chan<- timeseries.LogLine) {
	lr.terminated = false
	file, err := os.Open(lr.filepath)
	if err != nil {
		log.Fatalf("Error opening log file: %v", err)
	}
	defer file.Close()

	latestOffset, err := lr.offsetPersister.GetOffset(lr.filepath)
	if err != nil {
		log.Fatal(err)
	}
	lr.offset = latestOffset

	reader := bufio.NewReader(file)

	// Skip to the latest offset
	for i := int64(0); i < lr.offset; i++ {
		reader.ReadString('\n')
	}

	for !lr.terminated {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Fatal error scanning log file: %v\nTerminating\n", err)
				lr.Terminate()
			}
		} else {
			lr.offset = lr.offset + 1
			logLine, err := parser.ParseLogLine(line)
			if err == nil {
				logChan <- logLine
			}
		}
	}
}

func (lr *logReader) Terminate() (err error) {
	lr.terminated = true
	err = lr.offsetPersister.PersistOffset(lr.filepath, lr.offset)
	return
}
