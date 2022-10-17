package db_bench

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/google/uuid"
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
	Item        int       `json:"item"`
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

// createConnectPairs creates an N pairs. Pair is a document connected with an edge: Doc1 --> Edge --> Doc2.
func createConnectedPairs(ctx context.Context, db driver.Database, documentCollection, edgeCollection string, n int) ([]string, []string, int64, int64, error) {

	// Document handling.

	documentCol, err := db.Collection(ctx, documentCollection)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed getting collection")
	}

	var documents []arangoArtifact

	tm := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i < n; i++ {
		artifactFrom := arangoArtifact{
			Name:        fmt.Sprintf("artifact-from-%d", i),
			Description: fmt.Sprintf("description-%d", i),
			CreateTime:  tm,
		}

		artifactTo := arangoArtifact{
			Name:        fmt.Sprintf("artifact-to-%d", i),
			Description: fmt.Sprintf("description-%d", i),
			CreateTime:  tm,
		}

		documents = append(documents, artifactFrom, artifactTo)
		tm = tm.AddDate(0, 0, 1)
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

	edgeMetas, _, err := edgeCol.CreateDocuments(ctx, edges)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed creating edge")
	}

	edgeCount, err := edgeCol.Count(ctx)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed counting edges")
	}

	return documentMetas.Keys(), edgeMetas.Keys(), documentCount, edgeCount, nil
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

func queryAllArangoPairsOneYear(ctx context.Context, db driver.Database, documentCollection, edgeCollection string, year int) (int64, error) {

	queryString := fmt.Sprintf("FOR d IN %s FOR v IN OUTBOUND d._id %s FILTER DATE_YEAR(v.create_time) == %d RETURN v", documentCollection, edgeCollection, year)
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

func newChain(documentCollection string, size int) ([]arangoArtifact, []arangoEdge, error) {

	if size < 1 {
		return nil, nil, nil
	}

	key, err := uuid.NewUUID()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed creating uuid")
	}
	last := arangoArtifact{
		Key:         key.String(),
		Name:        "artifact-0",
		Description: "description-0",
		Item:        1,
		CreateTime:  time.Now(),
	}

	documents := []arangoArtifact{last}
	var edges []arangoEdge

	for i := 0; i < size-1; i++ {
		key, err := uuid.NewUUID()
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed creating uuid")
		}
		document := arangoArtifact{
			Key:         key.String(),
			Name:        fmt.Sprintf("artifact-%d", i+1),
			Description: fmt.Sprintf("description-%d", i+1),
			Item:        1,
			CreateTime:  time.Now(),
		}
		edge := arangoEdge{
			From: fmt.Sprintf("%s/%s", documentCollection, last.Key),
			To:   fmt.Sprintf("%s/%s", documentCollection, key),
			Body: fmt.Sprintf("body-%d", i),
		}
		documents = append(documents, document)
		edges = append(edges, edge)
		last = document
	}

	return documents, edges, nil
}

// createArangoChain creates a chain of documents connected by edges. You can specify the chain size and number of chains.
func createArangoChain(ctx context.Context, db driver.Database, documentCollection, edgeCollection string, size, n int) ([]string, []string, int64, int64, error) {

	// Document handling.

	documentCol, err := db.Collection(ctx, documentCollection)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed getting collection")
	}

	var documents []arangoArtifact
	var edges []arangoEdge

	for i := 0; i < n; i++ {
		ds, es, err := newChain(documentCollection, size)
		if err != nil {
			return nil, nil, 0, 0, errors.Wrap(err, "failed allocating graph")
		}

		documents = append(documents, ds...)
		edges = append(edges, es...)
	}

	documentMetas, _, err := documentCol.CreateDocuments(ctx, documents)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed creating document")
	}

	documentCount, err := documentCol.Count(ctx)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed counting documents")
	}

	// Edge handling.

	edgeCol, err := db.Collection(ctx, edgeCollection)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed getting collection")
	}

	edgeMetas, _, err := edgeCol.CreateDocuments(ctx, edges)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed creating edge")
	}

	edgeCount, err := edgeCol.Count(ctx)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed counting edges")
	}

	return documentMetas.Keys(), edgeMetas.Keys(), documentCount, edgeCount, nil
}

func queryArangoNeighbour(ctx context.Context, db driver.Database, documentCollection, edgeCollection string, key string, index int) (arangoArtifact, error) {
	queryString := fmt.Sprintf("FOR v IN %d..%d OUTBOUND '%s/%s' %s RETURN v", index, index, documentCollection, key, edgeCollection)
	newCTX := driver.WithQueryCount(ctx)
	cursor, err := db.Query(newCTX, queryString, nil)
	if err != nil {
		return arangoArtifact{}, errors.Wrap(err, "failed querying database")
	}
	defer cursor.Close()

	var document arangoArtifact

	_, err = cursor.ReadDocument(newCTX, &document)

	if driver.IsNoMoreDocuments(err) {
		return arangoArtifact{}, errors.New("no document found by query")
	}

	if err != nil {
		return arangoArtifact{}, errors.Wrap(err, "failed reading document")
	}

	return document, nil
}

func sumArangoChainNeighbourItems(ctx context.Context, db driver.Database, documentCollection, edgeCollection string, key string, index int) (int, error) {
	queryString := fmt.Sprintf("FOR d IN 0..%d OUTBOUND '%s/%s' %s COLLECT item = d.item INTO g RETURN SUM(g[*].d.item)", index, documentCollection, key, edgeCollection)
	newCTX := driver.WithQueryCount(ctx)
	cursor, err := db.Query(newCTX, queryString, nil)
	if err != nil {
		return 0, errors.Wrap(err, "failed querying database")
	}
	defer cursor.Close()

	var length int

	_, err = cursor.ReadDocument(newCTX, &length)

	if driver.IsNoMoreDocuments(err) {
		return 0, errors.New("no document found by query")
	}

	if err != nil {
		return 0, errors.Wrap(err, "failed reading document")
	}

	return length, nil
}

// createArangoNeighbours creates one parents and n neighbours (direct connection).
func createArangoNeighbours(ctx context.Context, db driver.Database, documentCollection, edgeCollection string, n int) ([]string, []string, int64, int64, error) {

	// Document handling.

	key, err := uuid.NewUUID()
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed creating uuid")
	}
	tm := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	parent := arangoArtifact{
		Key:         key.String(),
		Name:        fmt.Sprintf("artifact-0"),
		Description: fmt.Sprintf("description-0"),
		CreateTime:  tm,
	}

	documents := []arangoArtifact{parent}
	var edges []arangoEdge

	for i := 0; i < n-1; i++ {
		tm = tm.AddDate(0, 0, 1)

		key, err := uuid.NewUUID()
		if err != nil {
			return nil, nil, 0, 0, errors.Wrap(err, "failed creating uuid")
		}

		document := arangoArtifact{
			Key:         key.String(),
			Name:        fmt.Sprintf("artifact-%d", i+1),
			Description: fmt.Sprintf("description-%d", i+1),
			CreateTime:  tm,
		}

		edge := arangoEdge{
			From: fmt.Sprintf("%s/%s", documentCollection, parent.Key),
			To:   fmt.Sprintf("%s/%s", documentCollection, document.Key),
			Body: fmt.Sprintf("body-%d", i),
		}

		documents = append(documents, document)
		edges = append(edges, edge)
	}

	documentCol, err := db.Collection(ctx, documentCollection)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed getting collection")
	}

	documentMetas, _, err := documentCol.CreateDocuments(ctx, documents)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed creating document")
	}

	documentCount, err := documentCol.Count(ctx)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed counting documents")
	}

	// Edge handling.

	edgeCol, err := db.Collection(ctx, edgeCollection)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed getting collection")
	}

	edgeMetas, _, err := edgeCol.CreateDocuments(ctx, edges)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed creating edge")
	}

	edgeCount, err := edgeCol.Count(ctx)
	if err != nil {
		return nil, nil, 0, 0, errors.Wrap(err, "failed counting edges")
	}

	return documentMetas.Keys(), edgeMetas.Keys(), documentCount, edgeCount, nil
}

func queryArangoSortedNeighbours(ctx context.Context, db driver.Database, documentCollection, edgeCollection string, key string) (int64, error) {
	queryString := fmt.Sprintf("FOR d IN OUTBOUND '%s/%s' %s SORT d.name RETURN d", documentCollection, key, edgeCollection)
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
