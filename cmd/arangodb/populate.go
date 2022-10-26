package main

import (
	"context"
	"flag"
	dbBench "github.com/geomodular/db-bench"
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

	var endpoint string
	var n int
	var chunk int

	flag.StringVar(&endpoint, "endpoint", dbBench.ArangoEndpoint, "ArangoDB endpoint")
	flag.IntVar(&n, "n", 1000000, "the number of entries to generate inside DB")
	flag.IntVar(&chunk, "chunk", 10000, "maximum inserts of one bulk operation")
	flag.Parse()

	db, err := dbBench.InitArango(endpoint, dbBench.ArangoDB)
	if err != nil {
		return errors.Wrap(err, "failed initializing postgres connection")
	}

	err = dbBench.CreateArangoDocumentCollection(db, dbBench.ArangoDocumentTestCollection)
	if err != nil {
		return errors.Wrap(err, "failed creating document collection")
	}

	err = dbBench.CreateArangoEdgeCollection(db, dbBench.ArangoEdgeTestCollection)
	if err != nil {
		return errors.Wrap(err, "failed creating edge collection")
	}

	ctx := context.Background()

	col, err := db.Collection(ctx, dbBench.ArangoDocumentTestCollection)
	if err != nil {
		return errors.Wrap(err, "failed accessing document collection")
	}

	docCount, err := col.Count(ctx)
	if err != nil {
		return errors.Wrap(err, "failed counting documents")
	}

	docs := int(docCount)

	if docs >= n {
		return errors.New("db is already populated")
	}

	log.Info().Int("docs", docs).Msg("pre-feeding status")

	total := n - docs

	actual := 0
	for actual < total {
		bulkCount := chunk
		if actual+bulkCount > total {
			bulkCount = total - actual
		}
		if _, _, err := dbBench.CreateBulkArangoDocuments(ctx, db, dbBench.ArangoDocumentTestCollection, bulkCount); err != nil {
			return errors.Wrap(err, "failed creating artifacts")
		}
		actual += bulkCount
		log.Info().Int("count", actual).Float64("perc", (float64(actual+docs)/float64(n))*100.0).Msg("status")
	}

	return nil
}
