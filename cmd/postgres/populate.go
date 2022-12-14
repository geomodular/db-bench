package main

import (
	"database/sql"
	"flag"
	"fmt"
	dbBench "github.com/geomodular/db-bench"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"os"
)

func main() {
	if err := run(); err != nil {
		log.Error().Err(err).Msg("")
		os.Exit(1)
	}
	os.Exit(0)
}

func run() error {

	var postgresConnStr string
	var n int
	var chunk int

	flag.StringVar(&postgresConnStr, "host", dbBench.PostgresConnStr, "connection string to PostgreSQL")
	flag.IntVar(&n, "n", 1000000, "the number of entries to generate inside DB")
	flag.IntVar(&chunk, "chunk", 10000, "maximum inserts of one bulk operation")
	flag.Parse()

	db, err := dbBench.InitPostgres(postgresConnStr)
	if err != nil {
		return errors.Wrap(err, "failed initializing postgres connection")
	}

	if err := dbBench.CreatePostgresTestingTables(db); err != nil {
		return errors.Wrap(err, "failed creating postgres testing tables")
	}

	var rows int
	err = db.QueryRow("SELECT COUNT(*) FROM artifacts;").Scan(&rows)
	if err != nil {
		return errors.Wrap(err, "failed counting rows")
	}

	if rows >= n {
		return errors.New("db is already populated")
	}

	log.Info().Int("rows", rows).Msg("pre-feeding status")

	total := n - rows

	actual := 0
	for actual < total {
		bulkCount := chunk
		if actual+bulkCount > total {
			bulkCount = total - actual
		}
		if err := createBulkPostgresArtifacts(db, bulkCount); err != nil {
			return errors.Wrap(err, "failed creating artifacts")
		}
		actual += bulkCount
		log.Info().Int("count", actual).Float64("perc", (float64(actual+rows)/float64(n))*100.0).Msg("status")
	}

	return nil
}

func createBulkPostgresArtifacts(db *sql.DB, n int) error {

	var stmt string

	for i := 0; i < n; i++ {
		id, _ := uuid.NewUUID()
		name := fmt.Sprintf("name-%d", i)
		description := fmt.Sprintf("description-%d", i)
		stmt = stmt + fmt.Sprintf("INSERT INTO artifacts(id, \"name\", description) VALUES ('%s', '%s', '%s');", id, name, description)
	}

	_, err := db.Exec(stmt)
	if err != nil {
		return errors.Wrap(err, "failed inserting into table")
	}

	return nil
}
