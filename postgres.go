package db_bench

import (
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

func initPostgres(connStr string) (*sql.DB, error) {

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, errors.Wrap(err, "failed opening postgres connection")
	}

	return db, nil
}

func createPostgresTestingTables(db *sql.DB) error {

	artifactSTMT := `CREATE TABLE IF NOT EXISTS artifact 
(
    id           UUID PRIMARY KEY,
    "name"       TEXT NOT NULL,
    description  TEXT,
    created      TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP()
);`

	edgeSTMT := `CREATE TABLE IF NOT EXISTS edge
(
    id      SERIAL PRIMARY KEY,
    "from"  UUID REFERENCES artifact,
    "to"    UUID REFERENCES artifact,
    body    TEXT
);`

	_, err := db.Exec(artifactSTMT)
	if err != nil {
		return errors.Wrap(err, "failed creating artifact table")
	}

	_, err = db.Exec(edgeSTMT)
	if err != nil {
		return errors.Wrap(err, "failed creating edge table")
	}

	return nil
}

func createPostgresArtifacts(db *sql.DB, n int) ([]string, int, error) {

	tx, err := db.Begin()
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed creating transaction")
	}

	var ids []string

	for i := 0; i < n; i++ {

		stmt := `INSERT INTO artifact(id, "name", description) VALUES ($1, $2, $3);`

		id, _ := uuid.NewUUID()
		name := fmt.Sprintf("name-%d", i)
		description := fmt.Sprintf("description-%d", i)

		_, err := tx.Exec(stmt, id, name, description)
		if err != nil {
			return nil, 0, errors.Wrap(err, "failed inserting into table")
		}

		ids = append(ids, id.String())
	}

	err = tx.Commit()
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed committing transaction")
	}

	var counter int
	err = db.QueryRow("SELECT COUNT(*) FROM artifact;").Scan(&counter)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed counting rows")
	}

	return ids, counter, nil
}

func createBulkPostgresArtifacts(db *sql.DB, n int) ([]string, int, error) {

	var stmt string
	var ids []string

	for i := 0; i < n; i++ {
		id, _ := uuid.NewUUID()
		name := fmt.Sprintf("name-%d", i)
		description := fmt.Sprintf("description-%d", i)
		stmt = stmt + fmt.Sprintf("INSERT INTO artifact(id, \"name\", description) VALUES ('%s', '%s', '%s');", id, name, description)

		ids = append(ids, id.String())
	}

	_, err := db.Exec(stmt)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed inserting into table")
	}

	var counter int
	err = db.QueryRow("SELECT COUNT(*) FROM artifact;").Scan(&counter)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed counting rows")
	}

	return ids, counter, nil
}

func removeBulkPostgresArtifacts(db *sql.DB, ids []string) error {

	var stmt string

	for _, id := range ids {
		stmt = stmt + fmt.Sprintf("DELETE FROM artifact WHERE id='%s';", id)
	}

	_, err := db.Exec(stmt)
	if err != nil {
		return errors.Wrap(err, "failed removing artifacts")
	}

	return nil
}
