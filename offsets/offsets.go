// Package offsets provides functionality to store and remember offsets into files
package offsets

import (
	"database/sql"
)

const CreateOffsetsTableStmt = `
CREATE TABLE IF NOT EXISTS offsets (
  filename varchar(255) primary key,
  offset integer
)
`

type OffsetPersister struct {
	DB *sql.DB
}

func (op *OffsetPersister) PersistOffset(filename string, offset int64) (err error) {
	_, err = op.DB.Exec("INSERT INTO offsets (filename, offset) "+
		"VALUES ($1, $2) "+
		"ON CONFLICT(filename) DO UPDATE SET offset = $2", filename, offset)
	return
}

func (op *OffsetPersister) GetOffset(filename string) (offset int64, err error) {
	row := op.DB.QueryRow("SELECT offset FROM offsets WHERE filename LIKE $1", filename)
	err = row.Scan(&offset)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return
}
