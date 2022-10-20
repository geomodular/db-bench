package db_bench

import (
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"math/rand"
	"time"
)

func initPostgres(connStr string) (*sql.DB, error) {

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, errors.Wrap(err, "failed opening postgres connection")
	}

	return db, nil
}

func createPostgresTestingTables(db *sql.DB) error {

	artifactSTMT := `CREATE TABLE IF NOT EXISTS artifacts
(
    id           UUID PRIMARY KEY,
    "name"       TEXT NOT NULL,
    description  TEXT,
    item         INTEGER DEFAULT 1,
    create_time  TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP()
);`

	edgeSTMT := `CREATE TABLE IF NOT EXISTS edges
(
    id      UUID PRIMARY KEY,
    "from"  UUID REFERENCES artifacts,
    "to"    UUID REFERENCES artifacts,
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

		stmt := `INSERT INTO artifacts(id, "name", description) VALUES ($1, $2, $3);`

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
	err = db.QueryRow("SELECT COUNT(*) FROM artifacts;").Scan(&counter)
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
		stmt = stmt + fmt.Sprintf("INSERT INTO artifacts(id, \"name\", description) VALUES ('%s', '%s', '%s');", id, name, description)

		ids = append(ids, id.String())
	}

	_, err := db.Exec(stmt)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed inserting into table")
	}

	var counter int
	err = db.QueryRow("SELECT COUNT(*) FROM artifacts;").Scan(&counter)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed counting rows")
	}

	return ids, counter, nil
}

func removeBulkPostgresArtifacts(db *sql.DB, ids []string) error {

	var stmt string

	for _, id := range ids {
		stmt = stmt + fmt.Sprintf("DELETE FROM artifacts WHERE id='%s';", id)
	}

	_, err := db.Exec(stmt)
	if err != nil {
		return errors.Wrap(err, "failed removing artifacts")
	}

	return nil
}

func removeBulkPostgresEdges(db *sql.DB, ids []string) error {

	var stmt string

	for _, id := range ids {
		stmt = stmt + fmt.Sprintf("DELETE FROM edges WHERE id='%s';", id)
	}

	_, err := db.Exec(stmt)
	if err != nil {
		return errors.Wrap(err, "failed removing edges")
	}

	return nil
}

func updateOnePostgresArtifact(db *sql.DB, id string) error {

	i := rand.Intn(1000)
	name := fmt.Sprintf("new-name-%d", i)
	description := fmt.Sprintf("new-description-%d", i)
	stmt := fmt.Sprintf("UPDATE artifacts SET \"name\" = '%s', description = '%s' WHERE id = '%s';", name, description, id)

	_, err := db.Exec(stmt)
	if err != nil {
		return errors.Wrap(err, "failed updating entry")
	}

	return nil
}

func updateBulkPostgresArtifacts(db *sql.DB, ids []string) (int, error) {

	var stmt string

	for _, id := range ids {
		i := rand.Intn(1000)
		name := fmt.Sprintf("new-name-%d", i)
		description := fmt.Sprintf("new-description-%d", i)
		stmt = stmt + fmt.Sprintf("UPDATE artifacts SET \"name\" = '%s', description = '%s' WHERE id = '%s';", name, description, id)
	}

	_, err := db.Exec(stmt)
	if err != nil {
		return 0, errors.Wrap(err, "failed updating table")
	}

	var counter int
	err = db.QueryRow("SELECT COUNT(*) FROM artifacts;").Scan(&counter)
	if err != nil {
		return 0, errors.Wrap(err, "failed counting rows")
	}

	return counter, nil
}

func queryReadPostgresArtifacts(db *sql.DB, ids []string) error {

	var stmt string

	for _, id := range ids {
		stmt = stmt + fmt.Sprintf("SELECT name FROM artifacts WHERE id = '%s';", id)
	}

	rows, err := db.Query(stmt)
	if err != nil {
		return errors.Wrap(err, "failed reading table")
	}
	defer rows.Close()

	for rows.Next() {
		var name string

		err = rows.Scan(&name)
		if err != nil {
			return errors.Wrap(err, "failed scanning variables")
		}
	}

	return nil
}

func createPostgresConnectedPairs(db *sql.DB, n int) ([]string, []string, int, int, error) {

	var stmt string
	var artifactIDs []string
	var edgeIDs []string

	tm := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i < n; i++ {
		edgeID, _ := uuid.NewUUID()
		fromID, _ := uuid.NewUUID()
		toID, _ := uuid.NewUUID()
		fromName := fmt.Sprintf("name-from-%d", i)
		toName := fmt.Sprintf("name-to-%d", i)
		fromDesc := fmt.Sprintf("description-%d", i)
		toDesc := fmt.Sprintf("description-%d", i)
		body := fmt.Sprintf("body-%d", i)

		stmt += fmt.Sprintf("INSERT INTO artifacts(id, \"name\", description, create_time) VALUES ('%s', '%s', '%s', '%s');", fromID, fromName, fromDesc, tm.Format(time.RFC3339))
		stmt += fmt.Sprintf("INSERT INTO artifacts(id, \"name\", description, create_time) VALUES ('%s', '%s', '%s', '%s');", toID, toName, toDesc, tm.Format(time.RFC3339))
		stmt += fmt.Sprintf("INSERT INTO edges(id, \"from\", \"to\", body) VALUES ('%s', '%s', '%s', '%s');", edgeID, fromID, toID, body)

		artifactIDs = append(artifactIDs, fromID.String(), toID.String())
		edgeIDs = append(edgeIDs, edgeID.String())
		tm = tm.AddDate(0, 0, 1)
	}

	_, err := db.Exec(stmt)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed inserting into table")
	}

	var artifactCounter int
	var edgeCounter int

	err = db.QueryRow("SELECT COUNT(*) FROM artifacts;").Scan(&artifactCounter)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed counting rows in artifact table")
	}

	err = db.QueryRow("SELECT COUNT(*) FROM edges;").Scan(&edgeCounter)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed counting rows in edge table")
	}

	return artifactIDs, edgeIDs, artifactCounter, edgeCounter, nil
}

func queryAllPostgresPairs(db *sql.DB) (int, error) {

	// NOTE: Controversial comparing to Arango.

	stmt := "SELECT t.name FROM edges INNER JOIN artifacts f ON edges.from = f.id INNER JOIN artifacts t ON edges.to = t.id;"

	rows, err := db.Query(stmt)
	if err != nil {
		return 0, errors.Wrap(err, "failed reading table")
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var name string

		err = rows.Scan(&name)
		if err != nil {
			return 0, errors.Wrap(err, "failed scanning variables")
		}
		count += 1
	}

	return count, nil
}

func queryAllPostgresPairsOneYear(db *sql.DB, year int) (int, error) {

	// NOTE: Controversial comparing to Arango.

	stmt := fmt.Sprintf("SELECT t.name FROM edges INNER JOIN artifacts f ON edges.from = f.id INNER JOIN artifacts t ON edges.to = t.id WHERE date_part('year', t.create_time) = %d;", year)

	rows, err := db.Query(stmt)
	if err != nil {
		return 0, errors.Wrap(err, "failed reading table")
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var name string

		err = rows.Scan(&name)
		if err != nil {
			return 0, errors.Wrap(err, "failed scanning variables")
		}
		count += 1
	}

	return count, nil
}

func createPostgresChain(db *sql.DB, n int) ([]string, []string, int, int, error) {

	var stmt string

	lastID, _ := uuid.NewUUID()
	stmt += fmt.Sprintf("INSERT INTO artifacts(id, \"name\", description) VALUES ('%s', 'name-0', 'description-0');", lastID)

	artifactIDs := []string{lastID.String()}
	var edgeIDs []string

	for i := 0; i < n-1; i++ {
		edgeID, _ := uuid.NewUUID()
		artifactID, _ := uuid.NewUUID()
		name := fmt.Sprintf("name-%d", i+1)
		desc := fmt.Sprintf("description-%d", i+1)
		body := fmt.Sprintf("body-%d", i)

		stmt += fmt.Sprintf("INSERT INTO artifacts(id, \"name\", description) VALUES ('%s', '%s', '%s');", artifactID, name, desc)
		stmt += fmt.Sprintf("INSERT INTO edges(id, \"from\", \"to\", body) VALUES ('%s', '%s', '%s', '%s');", edgeID, lastID, artifactID, body)

		artifactIDs = append(artifactIDs, artifactID.String())
		edgeIDs = append(edgeIDs, edgeID.String())

		lastID = artifactID
	}

	_, err := db.Exec(stmt)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed inserting into table")
	}

	var artifactCounter int
	var edgeCounter int

	err = db.QueryRow("SELECT COUNT(*) FROM artifacts;").Scan(&artifactCounter)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed counting rows in artifact table")
	}

	err = db.QueryRow("SELECT COUNT(*) FROM edges;").Scan(&edgeCounter)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed counting rows in edge table")
	}

	return artifactIDs, edgeIDs, artifactCounter, edgeCounter, nil
}

func queryPostgresNeighbourN(db *sql.DB, startingID string, i int) (string, string, error) {

	// NOTE: Controversial comparing to Arango.

	stmt := `
WITH RECURSIVE neighbours(id, name, n) as (
    SELECT id, name, 0 FROM artifacts WHERE id = '%s'
UNION
    SELECT e.to, a.name, n+1 FROM edges e INNER JOIN neighbours n ON e.from = n.id INNER JOIN artifacts a ON e.to = a.id WHERE n < %d
) SELECT * FROM neighbours LIMIT 1 OFFSET %d;
`

	stmt = fmt.Sprintf(stmt, startingID, i, i)

	var id string
	var name string
	var n int

	err := db.QueryRow(stmt).Scan(&id, &name, &n)
	if err != nil {
		return "", "", errors.Wrap(err, "failed searching in chain")
	}

	return id, name, nil
}

func sumPostgresNeighbourNItems(db *sql.DB, startingID string, i int) (int, error) {

	// NOTE: Controversial comparing to Arango.

	stmt := `
WITH RECURSIVE neighbours(id, name, item, n) as (
    SELECT id, name, item, 0 FROM artifacts WHERE id = '%s'
UNION
    SELECT e.to, a.name, a.item, n+1 FROM edges e INNER JOIN neighbours n ON e.from = n.id INNER JOIN artifacts a ON e.to = a.id WHERE n < %d
) SELECT sum(item) FROM neighbours;
`

	stmt = fmt.Sprintf(stmt, startingID, i)

	var sum int

	err := db.QueryRow(stmt).Scan(&sum)
	if err != nil {
		return 0, errors.Wrap(err, "failed searching in chain")
	}

	return sum, nil
}

func createPostgresNeighbours(db *sql.DB, n int) ([]string, []string, int, int, error) {

	var stmt string

	firstID, _ := uuid.NewUUID()
	stmt += fmt.Sprintf("INSERT INTO artifacts(id, \"name\", description) VALUES ('%s', 'name-0', 'description-0');", firstID)

	artifactIDs := []string{firstID.String()}
	var edgeIDs []string

	for i := 0; i < n-1; i++ {
		edgeID, _ := uuid.NewUUID()
		artifactID, _ := uuid.NewUUID()
		name := fmt.Sprintf("name-%d", i+1)
		desc := fmt.Sprintf("description-%d", i+1)
		body := fmt.Sprintf("body-%d", i)

		stmt += fmt.Sprintf("INSERT INTO artifacts(id, \"name\", description) VALUES ('%s', '%s', '%s');", artifactID, name, desc)
		stmt += fmt.Sprintf("INSERT INTO edges(id, \"from\", \"to\", body) VALUES ('%s', '%s', '%s', '%s');", edgeID, firstID, artifactID, body)

		artifactIDs = append(artifactIDs, artifactID.String())
		edgeIDs = append(edgeIDs, edgeID.String())
	}

	_, err := db.Exec(stmt)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed inserting into table")
	}

	var artifactCounter int
	var edgeCounter int

	err = db.QueryRow("SELECT COUNT(*) FROM artifacts;").Scan(&artifactCounter)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed counting rows in artifact table")
	}

	err = db.QueryRow("SELECT COUNT(*) FROM edges;").Scan(&edgeCounter)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed counting rows in edge table")
	}

	return artifactIDs, edgeIDs, artifactCounter, edgeCounter, nil
}

func queryPostgresSortedNeighbours(db *sql.DB, id string) (int, error) {

	// NOTE: Controversial comparing to Arango.

	stmt := fmt.Sprintf("SELECT a.name FROM edges e INNER JOIN artifacts a ON e.to = a.id WHERE e.from = '%s' GROUP BY a.name;", id)

	rows, err := db.Query(stmt)
	if err != nil {
		return 0, errors.Wrap(err, "failed reading table")
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var name string

		err = rows.Scan(&name)
		if err != nil {
			return 0, errors.Wrap(err, "failed scanning variables")
		}
		count += 1
	}

	return count, nil
}
