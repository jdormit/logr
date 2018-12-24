package offsets

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"testing"
)

func loadDB() (db *sql.DB, err error) {
	db, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		return
	}
	_, err = db.Exec(CreateOffsetsTableStmt)
	return
}

func validateErr(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
	}
}

func expectOffset(t *testing.T, expected int64, actual int64, err error) {
	validateErr(t, err)
	if actual != expected {
		t.Errorf("Expected %d, but found %d\n", expected, actual)
	}
}

func TestPersistOffset(t *testing.T) {
	db, err := loadDB()
	if err != nil {
		t.Error(err)
	}
	op := OffsetPersister{db}
	err = op.PersistOffset("thefile", 100)
	validateErr(t, err)
	offset, err := op.GetOffset("thefile")
	expectOffset(t, 100, offset, err)

	err = op.PersistOffset("thefile", 90)
	validateErr(t, err)
	offset, err = op.GetOffset("thefile")
	expectOffset(t, 90, offset, err)

	offset, err = op.GetOffset("notthefile")
	expectOffset(t, 0, offset, err)

	err = op.PersistOffset("", 5)
	validateErr(t, err)
	offset, err = op.GetOffset("")
	expectOffset(t, 5, offset, err)
}
