package db_bench

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/pkg/errors"
	"math/rand"
	"time"
)

type arangoArtifact struct {

	// Mandatory `key` field.
	Key string `json:"_key,omitempty"`

	// Other fields.
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreateTime  time.Time `json:"create_time"`
}

type arangoEdge struct {

	// Mandatory `key` field.
	Key string `json:"_key,omitempty"`

	// Edge fields.
	From string `json:"_from"`
	To   string `json:"_to"`

	// Other random fields.
	Body string `json:"body"`
}

func initArango(endpoint, dbName string) (driver.Database, error) {
	conn, err := http.NewConnection(http.ConnectionConfig{Endpoints: []string{endpoint}})
	if err != nil {
		return nil, errors.Wrap(err, "failed connecting to arangodb")
	}

	client, err := driver.NewClient(driver.ClientConfig{Connection: conn})
	if err != nil {
		return nil, errors.Wrap(err, "failed creating a client")
	}

	dbExists, err := client.DatabaseExists(nil, dbName)
	if err != nil {
		return nil, errors.Wrap(err, "failed checking for db existence")
	}

	var db driver.Database
	if !dbExists {
		db, err = client.CreateDatabase(nil, dbName, nil)
		if err != nil {
			return nil, errors.Wrap(err, "failed creating database")
		}
	} else {
		db, err = client.Database(nil, dbName)
		if err != nil {
			return nil, errors.Wrap(err, "failed opening database")
		}
	}

	return db, nil
}

func createArangoDocumentCollection(db driver.Database, collection string) error {

	colExists, err := db.CollectionExists(nil, collection)
	if err != nil {
		return errors.Wrap(err, "failed checking for collection existence")
	}

	if !colExists {
		_, err = db.CreateCollection(nil, collection, nil)
		if err != nil {
			return errors.Wrap(err, "failed creating collection")
		}
	}

	return nil
}

func createArangoEdgeCollection(db driver.Database, collection string) error {

	colExists, err := db.CollectionExists(nil, collection)
	if err != nil {
		return errors.Wrap(err, "failed checking for collection existence")
	}

	if !colExists {
		_, err = db.CreateCollection(nil, collection, &driver.CreateCollectionOptions{
			Type: driver.CollectionTypeEdge,
		})
		if err != nil {
			return errors.Wrap(err, "failed creating collection")
		}
	}

	return nil
}

func createArangoDocuments(ctx context.Context, db driver.Database, collection string, n int) ([]string, int64, error) {

	col, err := db.Collection(ctx, collection)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed getting collection")
	}

	var keys []string

	// TODO: Do it in transaction?

	for i := 0; i < n; i++ {
		artifact := arangoArtifact{
			Name:        fmt.Sprintf("artifact-%d", i),
			Description: fmt.Sprintf("description-%d", i),
			CreateTime:  time.Now(),
		}

		meta, err := col.CreateDocument(ctx, &artifact)
		if err != nil {
			return nil, 0, errors.Wrap(err, "failed creating document")
		}

		keys = append(keys, meta.Key)
	}

	count, err := col.Count(ctx)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed counting documents")
	}

	return keys, count, nil
}

func createBulkArangoDocuments(ctx context.Context, db driver.Database, collection string, n int) ([]string, int64, error) {

	col, err := db.Collection(ctx, collection)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed getting collection")
	}

	var documents []arangoArtifact

	for i := 0; i < n; i++ {
		artifact := arangoArtifact{
			Name:        fmt.Sprintf("artifact-%d", i),
			Description: fmt.Sprintf("description-%d", i),
			CreateTime:  time.Now(),
		}
		documents = append(documents, artifact)
	}

	metaSlice, _, err := col.CreateDocuments(ctx, documents)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed creating documents")
	}

	count, err := col.Count(ctx)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed counting documents")
	}

	return metaSlice.Keys(), count, nil
}

func readOneArangoDocument(ctx context.Context, db driver.Database, collection string, key string) error {

	col, err := db.Collection(ctx, collection)
	if err != nil {
		return errors.Wrap(err, "failed getting collection")
	}

	var document arangoArtifact

	_, err = col.ReadDocument(ctx, key, &document)
	if err != nil {
		return errors.Wrap(err, "failed reading document")
	}

	return nil
}

func readBulkArangoDocuments(ctx context.Context, db driver.Database, collection string, keys []string) (int, error) {

	col, err := db.Collection(ctx, collection)
	if err != nil {
		return 0, errors.Wrap(err, "failed getting collection")
	}

	// It is required to allocate the array to the correct size.
	documents := make([]arangoArtifact, len(keys))

	metas, _, err := col.ReadDocuments(ctx, keys, documents)
	if err != nil {
		return 0, errors.Wrap(err, "failed reading documents")
	}

	return len(metas.Keys()), nil
}

func updateOneArangoDocument(ctx context.Context, db driver.Database, collection string, key string) error {

	col, err := db.Collection(ctx, collection)
	if err != nil {
		return errors.Wrap(err, "failed getting collection")
	}

	i := rand.Intn(1000)
	document := arangoArtifact{
		Name:        fmt.Sprintf("new-artifact-%d", i),
		Description: fmt.Sprintf("new-description-%d", i),
		CreateTime:  time.Now(),
	}

	_, err = col.UpdateDocument(ctx, key, &document)
	if err != nil {
		return errors.Wrap(err, "failed updating document")
	}

	return nil
}

func updateBulkArangoDocuments(ctx context.Context, db driver.Database, collection string, keys []string) (int, error) {

	col, err := db.Collection(ctx, collection)
	if err != nil {
		return 0, errors.Wrap(err, "failed getting collection")
	}

	var documents []arangoArtifact

	n := len(keys)
	for i := 0; i < n; i++ {
		i := rand.Intn(1000)
		artifact := arangoArtifact{
			Name:        fmt.Sprintf("new-artifact-%d", i),
			Description: fmt.Sprintf("new-description-%d", i),
			CreateTime:  time.Now(),
		}
		documents = append(documents, artifact)
	}

	metas, _, err := col.UpdateDocuments(ctx, keys, documents)
	if err != nil {
		return 0, errors.Wrap(err, "failed updating document")
	}

	return len(metas.Keys()), nil
}

func queryArangoDocuments(ctx context.Context, db driver.Database, collection string, keys []string) (int64, error) {

	queryString := fmt.Sprintf("FOR d IN %s RETURN d", collection)
	newCTX := driver.WithQueryCount(ctx)
	cursor, err := db.Query(newCTX, queryString, nil)
	if err != nil {
		return 0, errors.Wrap(err, "failed querying database")
	}
	defer cursor.Close()

	for {
		var document arangoArtifact

		_, err := cursor.ReadDocument(newCTX, &document)

		if driver.IsNoMoreDocuments(err) {
			break
		}

		if err != nil {
			return 0, errors.Wrap(err, "failed reading document")
		}
	}

	return cursor.Count(), nil
}

func createConnectedPairs(ctx context.Context, db driver.Database, documentCollection, edgeCollection string, n int) ([]string, []string, int64, int64, error) {

	// Document handling.

	documentCol, err := db.Collection(ctx, documentCollection)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed getting collection")
	}

	var documents []arangoArtifact

	for i := 0; i < n; i++ {
		artifactFrom := arangoArtifact{
			Name:        fmt.Sprintf("artifact-from-%d", i),
			Description: fmt.Sprintf("description-%d", i),
			CreateTime:  time.Now(),
		}

		artifactTo := arangoArtifact{
			Name:        fmt.Sprintf("artifact-to-%d", i),
			Description: fmt.Sprintf("description-%d", i),
			CreateTime:  time.Now(),
		}

		documents = append(documents, artifactFrom, artifactTo)
	}

	documentMetas, _, err := documentCol.CreateDocuments(ctx, documents)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed creating document")
	}
	documentIDs := documentMetas.IDs()

	documentCount, err := documentCol.Count(ctx)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed counting documents")
	}

	// Edge handling.

	edgeCol, err := db.Collection(ctx, edgeCollection)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed getting collection")
	}

	var edges []arangoEdge

	for i := 0; i < n; i++ {

		j := i * 2

		edge := arangoEdge{
			From: documentIDs[j].String(),
			To:   documentIDs[j+1].String(),
			Body: fmt.Sprintf("body-%d", i),
		}

		edges = append(edges, edge)
	}

	edgesMeta, _, err := edgeCol.CreateDocuments(ctx, edges)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed creating edge")
	}

	edgeCount, err := edgeCol.Count(ctx)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed counting edges")
	}

	return documentMetas.Keys(), edgesMeta.Keys(), documentCount, edgeCount, nil
}

func queryAllArangoPairs(ctx context.Context, db driver.Database, documentCollection, edgeCollection string) (int64, error) {

	queryString := fmt.Sprintf("FOR d IN %s FOR v IN OUTBOUND d._id %s RETURN v", documentCollection, edgeCollection)
	newCTX := driver.WithQueryCount(ctx)
	cursor, err := db.Query(newCTX, queryString, nil)
	if err != nil {
		return 0, errors.Wrap(err, "failed querying database")
	}
	defer cursor.Close()

	for {
		var document arangoArtifact

		_, err := cursor.ReadDocument(newCTX, &document)

		if driver.IsNoMoreDocuments(err) {
			break
		}

		if err != nil {
			return 0, errors.Wrap(err, "failed reading document")
		}
	}

	return cursor.Count(), nil
}
