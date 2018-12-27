package reader

import (
	"database/sql"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/jdormit/logr/offsets"
	"github.com/jdormit/logr/timeseries"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"testing"
	"time"
)

const logPath = "./example.log"

func parseTime(timeStr string) time.Time {
	time, err := time.Parse("02/Jan/2006:15:04:05 -0700", timeStr)
	if err != nil {
		log.Fatal(err)
	}
	return time
}

func loadDB(nonce string) (db *sql.DB, err error) {
	// We want each test case to have its own in-memory db, but the db
	// needs to be shared between all goroutines for the test case, so
	// we give each test case a unique db name and cache=shared.
	// See https://github.com/mattn/go-sqlite3/issues/204
	db, err = sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=memory&cache=shared", nonce))
	if err != nil {
		return
	}
	_, err = db.Exec(offsets.CreateOffsetsTableStmt)
	return
}

func awaitLogLine(t *testing.T, c <-chan timeseries.LogLine, timeout int) timeseries.LogLine {
	select {
	case logLine := <-c:
		return logLine
	case <-time.After(time.Duration(timeout) * time.Second):
		t.Fatalf("Did not receive log line after %d seconds", timeout)
	}
	panic("Did not time out or receive log line")
}

func TestTailLogFile(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	t.Run("basic", func(t *testing.T) {
		os.Remove(logPath)
		os.Create(logPath)
		db, err := loadDB("basic")
		if err != nil {
			t.Error(err)
			return
		}
		offsetPersister := offsets.OffsetPersister{db}
		logReader := NewLogReader(&offsetPersister, logPath)
		logChan := make(chan timeseries.LogLine)
		go logReader.TailLogFile(logChan)
		defer logReader.Terminate()
		file, err := os.OpenFile(logPath, os.O_RDWR, 0644)
		if err != nil {
			t.Error(err)
			return
		}
		file.WriteString("127.0.0.1 - james [09/May/2018:16:00:39 +0000] " +
			"\"GET /report HTTP/1.0\" 200 123\n")
		logLine := awaitLogLine(t, logChan, 2)
		expected := timeseries.LogLine{
			"127.0.0.1",
			"-",
			"james",
			parseTime("09/May/2018:16:00:39 +0000"),
			"GET",
			"/report",
			200,
			123,
		}
		if !cmp.Equal(logLine, expected) {
			t.Errorf("Expected: %#v\nActual: %#v\n", expected, logLine)
		}
		file.WriteString("127.0.0.1 - jill [09/May/2018:16:00:41 +0000] " +
			"\"GET /api/user HTTP/1.0\" 200 234\n")
		logLine = awaitLogLine(t, logChan, 2)
		expected = timeseries.LogLine{
			"127.0.0.1",
			"-",
			"jill",
			parseTime("09/May/2018:16:00:41 +0000"),
			"GET",
			"/api/user",
			200,
			234,
		}
		if !cmp.Equal(logLine, expected) {
			t.Errorf("Expected: %#v\nActual: %#v\n", expected, logLine)
		}
	})

	t.Run("offset persistence", func(t *testing.T) {
		os.Remove(logPath)
		os.Create(logPath)
		db, err := loadDB("offsetpersistence")
		if err != nil {
			t.Error(err)
			return
		}
		offsetPersister := offsets.OffsetPersister{db}
		logReader := NewLogReader(&offsetPersister, logPath)
		logChan := make(chan timeseries.LogLine)
		go logReader.TailLogFile(logChan)
		file, err := os.OpenFile(logPath, os.O_RDWR, 0644)
		if err != nil {
			t.Error(err)
			return
		}
		file.WriteString("127.0.0.1 - james [09/May/2018:16:00:39 +0000] " +
			"\"GET /report HTTP/1.0\" 200 123\n")
		awaitLogLine(t, logChan, 2)
		logReader.Terminate()
		file.WriteString("127.0.0.1 - jill [09/May/2018:16:00:41 +0000] " +
			"\"GET /api/user HTTP/1.0\" 200 234\n")
		go logReader.TailLogFile(logChan)
		defer logReader.Terminate()
		logLine := awaitLogLine(t, logChan, 2)
		expected := timeseries.LogLine{
			"127.0.0.1",
			"-",
			"jill",
			parseTime("09/May/2018:16:00:41 +0000"),
			"GET",
			"/api/user",
			200,
			234,
		}
		if !cmp.Equal(logLine, expected) {
			t.Errorf("Expected: %#v\nActual: %#v\n", expected, logLine)
		}
	})

	os.Remove(logPath)
}
