package reader

import (
	"os"
	"testing"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/jdormit/logr/offsets"
	"github.com/jdormit/logr/timeseries"
	"github.com/google/go-cmp/cmp"
	"time"
	"log"
)

const logPath = "./example.log"

func parseTime(timeStr string) time.Time {
	time, err := time.Parse("02/Jan/2006:15:04:05 -0700", timeStr)
	if err != nil {
		log.Fatal(err)
	}
	return time
}

func loadDB() (db *sql.DB, err error) {
	db, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		return
	}
	_, err = db.Exec(offsets.CreateOffsetsTableStmt)
	return
}

func TestTailLogFile(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	os.Remove(logPath)
	os.Create(logPath)

	t.Run("basic", func(t *testing.T) {
		db, err := loadDB()
		if err != nil {
			t.Error(err)
			return
		}
		offsetPersister := offsets.OffsetPersister{db}
		logReader := NewLogReader(&offsetPersister)
		logChan := make(chan timeseries.LogLine)
		go logReader.TailLogFile(logPath, logChan)
		defer logReader.Terminate()
		file, err := os.OpenFile(logPath, os.O_RDWR, 0644)
		if err != nil {
			t.Error(err)
			return
		}
		file.WriteString("127.0.0.1 - james [09/May/2018:16:00:39 +0000] "+
			"\"GET /report HTTP/1.0\" 200 123\n")
		logLine := <- logChan
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
	})

	t.Run("offset persistence", func(t *testing.T) {
		db, err := loadDB()
		if err != nil {
			t.Error(err)
			return
		}
		offsetPersister := offsets.OffsetPersister{db}
		logReader := NewLogReader(&offsetPersister)
		logChan := make(chan timeseries.LogLine)
		go logReader.TailLogFile(logPath, logChan)
		file, err := os.OpenFile(logPath, os.O_RDWR, 0644)
		if err != nil {
			t.Error(err)
			return
		}
		file.WriteString("127.0.0.1 - james [09/May/2018:16:00:39 +0000] "+
			"\"GET /report HTTP/1.0\" 200 123\n")
		logReader.Terminate()
		file.WriteString("127.0.0.1 - jill [09/May/2018:16:00:41 +0000] "+
			"\"GET /api/user HTTP/1.0\" 200 234\n")
		logReader.TailLogFile(logPath, logChan)
		defer logReader.Terminate()
		logLine := <- logChan
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
