package db_bench

import (
	"context"
	"github.com/arangodb/go-driver"
	"github.com/stretchr/testify/suite"
	"sort"
	"testing"
)

const (
	arangoEndpoint               = "http://localhost:8529"
	arangoDB                     = "testdb"
	arangoDocumentTestCollection = "testdocumentcollection"
	arangoEdgeTestCollection     = "testedgecollection"
)

type arangoSuite struct {
	suite.Suite

	db                       driver.Database
	documentKeysToCleanNow   []string
	documentKeysToCleanLater []string
	edgeKeysToCleanNow       []string
	edgeKeysToCleanLater     []string
}

func (s *arangoSuite) SetupSuite() {

	db, err := initArango(arangoEndpoint, arangoDB)
	s.Require().NoError(err)

	err = createArangoDocumentCollection(db, arangoDocumentTestCollection)
	s.Require().NoError(err)

	err = createArangoEdgeCollection(db, arangoEdgeTestCollection)
	s.Require().NoError(err)

	s.db = db
}

func (s *arangoSuite) TearDownTest() {

	ctx := context.Background()

	if s.documentKeysToCleanNow != nil {

		col, err := s.db.Collection(ctx, arangoDocumentTestCollection)
		s.Require().NoError(err)

		_, _, err = col.RemoveDocuments(ctx, s.documentKeysToCleanNow)
		s.Require().NoError(err)

		s.documentKeysToCleanNow = nil
	}

	if s.edgeKeysToCleanNow != nil {

		col, err := s.db.Collection(ctx, arangoEdgeTestCollection)
		s.Require().NoError(err)

		_, _, err = col.RemoveDocuments(ctx, s.edgeKeysToCleanNow)
		s.Require().NoError(err)

		s.edgeKeysToCleanNow = nil
	}
}

func (s *arangoSuite) TearDownSuite() {

	ctx := context.Background()

	if s.documentKeysToCleanLater != nil {

		col, err := s.db.Collection(ctx, arangoDocumentTestCollection)
		s.Require().NoError(err)

		_, _, err = col.RemoveDocuments(ctx, s.documentKeysToCleanLater)
		s.Require().NoError(err)

		s.documentKeysToCleanLater = nil
	}

	if s.edgeKeysToCleanLater != nil {

		col, err := s.db.Collection(ctx, arangoEdgeTestCollection)
		s.Require().NoError(err)

		_, _, err = col.RemoveDocuments(ctx, s.edgeKeysToCleanLater)
		s.Require().NoError(err)

		s.edgeKeysToCleanLater = nil
	}
}

func (s *arangoSuite) Test01_Create10() {

	documentCount := 10
	ctx := context.Background()

	keys, count, err := createArangoDocuments(ctx, s.db, arangoDocumentTestCollection, documentCount)
	s.Require().NoError(err)
	s.EqualValues(documentCount, count)

	// Clean up.
	s.documentKeysToCleanNow = keys
}

func (s *arangoSuite) Test02_Create100() {

	documentCount := 100
	ctx := context.Background()

	keys, count, err := createArangoDocuments(ctx, s.db, arangoDocumentTestCollection, documentCount)
	s.Require().NoError(err)
	s.EqualValues(documentCount, count)

	// Clean up.
	s.documentKeysToCleanNow = keys
}

func (s *arangoSuite) Test03_Create1000() {

	documentCount := 1000
	ctx := context.Background()

	keys, count, err := createArangoDocuments(ctx, s.db, arangoDocumentTestCollection, documentCount)
	s.Require().NoError(err)
	s.EqualValues(documentCount, count)

	// Clean up.
	s.documentKeysToCleanNow = keys
}

func (s *arangoSuite) Test04_BulkCreate1000() {

	documentCount := 1000
	ctx := context.Background()

	keys, count, err := createBulkArangoDocuments(ctx, s.db, arangoDocumentTestCollection, documentCount)
	s.Require().NoError(err)
	s.EqualValues(documentCount, count)

	// Clean up.
	s.documentKeysToCleanNow = keys
}

func (s *arangoSuite) Test05_BulkCreate10000() {

	documentCount := 10000
	ctx := context.Background()

	keys, count, err := createBulkArangoDocuments(ctx, s.db, arangoDocumentTestCollection, documentCount)
	s.Require().NoError(err)
	s.EqualValues(documentCount, count)

	// Clean up.
	s.documentKeysToCleanLater = keys
}

func (s *arangoSuite) Test06_Read10000() {

	ctx := context.Background()
	for _, k := range s.documentKeysToCleanLater {
		err := readOneArangoDocument(ctx, s.db, arangoDocumentTestCollection, k)
		s.Require().NoError(err)
	}
}

func (s *arangoSuite) Test07_BulkRead10000() {

	ctx := context.Background()

	count, err := readBulkArangoDocuments(ctx, s.db, arangoDocumentTestCollection, s.documentKeysToCleanLater)
	s.Require().NoError(err)
	s.EqualValues(len(s.documentKeysToCleanLater), count)
}

func (s *arangoSuite) Test08_Update10000() {

	ctx := context.Background()
	for _, k := range s.documentKeysToCleanLater {
		err := updateOneArangoDocument(ctx, s.db, arangoDocumentTestCollection, k)
		s.Require().NoError(err)
	}
}

func (s *arangoSuite) Test09_BulkUpdate10000() {

	ctx := context.Background()

	count, err := updateBulkArangoDocuments(ctx, s.db, arangoDocumentTestCollection, s.documentKeysToCleanLater)
	s.Require().NoError(err)
	s.EqualValues(len(s.documentKeysToCleanLater), count)
}

func (s *arangoSuite) Test10_QueryRead10000() {

	ctx := context.Background()

	count, err := queryArangoDocuments(ctx, s.db, arangoDocumentTestCollection, s.documentKeysToCleanLater)
	s.Require().NoError(err)
	s.EqualValues(len(s.documentKeysToCleanLater), count)

	s.documentKeysToCleanNow = s.documentKeysToCleanLater
	s.documentKeysToCleanLater = nil
}

func (s *arangoSuite) Test11_CreateConnectedPairs10() {

	ctx := context.Background()

	documentKeys, edgeKeys, documentCount, edgeCount, err := createConnectedPairs(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, 10)
	s.Require().NoError(err)
	s.EqualValues(20, documentCount)
	s.EqualValues(10, edgeCount)

	s.documentKeysToCleanNow = documentKeys
	s.edgeKeysToCleanNow = edgeKeys
}

func (s *arangoSuite) Test12_CreateConnectedPairs100() {

	ctx := context.Background()

	documentKeys, edgeKeys, documentCount, edgeCount, err := createConnectedPairs(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, 100)
	s.Require().NoError(err)
	s.EqualValues(200, documentCount)
	s.EqualValues(100, edgeCount)

	s.documentKeysToCleanNow = documentKeys
	s.edgeKeysToCleanNow = edgeKeys
}

func (s *arangoSuite) Test13_CreateConnectedPairs10000() {

	ctx := context.Background()

	documentKeys, edgeKeys, documentCount, edgeCount, err := createConnectedPairs(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, 10000)
	s.Require().NoError(err)
	s.EqualValues(20000, documentCount)
	s.EqualValues(10000, edgeCount)

	s.documentKeysToCleanLater = documentKeys
	s.edgeKeysToCleanLater = edgeKeys
}

func (s *arangoSuite) Test14_QueryAllConnectedPairs10000() {
	ctx := context.Background()

	count, err := queryAllArangoPairs(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection)
	s.Require().NoError(err)
	s.EqualValues(10000, count)
}

func (s *arangoSuite) Test15_QueryAllConnectedPaisOneYear10000() {
	ctx := context.Background()

	count, err := queryAllArangoPairsOneYear(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, 2022)
	s.Require().NoError(err)
	s.EqualValues(365, count)

	s.documentKeysToCleanNow = s.documentKeysToCleanLater
	s.edgeKeysToCleanNow = s.edgeKeysToCleanLater
	s.documentKeysToCleanLater = nil
	s.edgeKeysToCleanLater = nil
}

func (s *arangoSuite) Test16_Chain1x10000() {
	ctx := context.Background()

	documentKeys, edgeKeys, documentCount, edgeCount, err := createArangoChain(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, 10000, 1)
	s.Require().NoError(err)
	s.EqualValues(10000, documentCount)
	s.EqualValues(9999, edgeCount)

	s.documentKeysToCleanLater = documentKeys
	s.edgeKeysToCleanLater = edgeKeys
}

func (s *arangoSuite) Test16_QueryNeighbour10() {
	ctx := context.Background()

	document, err := queryArangoNeighbour(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, s.documentKeysToCleanLater[0], 10)
	s.Require().NoError(err)
	s.EqualValues("artifact-10", document.Name)
	s.EqualValues(s.documentKeysToCleanLater[10], document.Key)
}

func (s *arangoSuite) Test17_QueryNeighbour100() {
	ctx := context.Background()

	document, err := queryArangoNeighbour(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, s.documentKeysToCleanLater[0], 100)
	s.Require().NoError(err)
	s.EqualValues("artifact-100", document.Name)
	s.EqualValues(s.documentKeysToCleanLater[100], document.Key)
}

func (s *arangoSuite) Test18_QueryNeighbour1000() {
	ctx := context.Background()

	document, err := queryArangoNeighbour(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, s.documentKeysToCleanLater[0], 1000)
	s.Require().NoError(err)
	s.EqualValues("artifact-1000", document.Name)
	s.EqualValues(s.documentKeysToCleanLater[1000], document.Key)
}

func (s *arangoSuite) Test19_QueryNeighbour2000() {
	ctx := context.Background()

	document, err := queryArangoNeighbour(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, s.documentKeysToCleanLater[0], 2000)
	s.Require().NoError(err)
	s.EqualValues("artifact-2000", document.Name)
	s.EqualValues(s.documentKeysToCleanLater[2000], document.Key)
}

func (s *arangoSuite) Test20_QueryNeighbour5000() {
	ctx := context.Background()

	document, err := queryArangoNeighbour(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, s.documentKeysToCleanLater[0], 5000)
	s.Require().NoError(err)
	s.EqualValues("artifact-5000", document.Name)
	s.EqualValues(s.documentKeysToCleanLater[5000], document.Key)
}

func (s *arangoSuite) Test21_QueryNeighbour9999() {
	ctx := context.Background()

	document, err := queryArangoNeighbour(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, s.documentKeysToCleanLater[0], 9999)
	s.Require().NoError(err)
	s.EqualValues("artifact-9999", document.Name)
	s.EqualValues(s.documentKeysToCleanLater[len(s.documentKeysToCleanLater)-1], document.Key)
}

func (s *arangoSuite) HandleStats(suiteName string, stats *suite.SuiteInformation) {

	s.T().Logf("=== %s", suiteName)

	keys := make([]string, 0, len(stats.TestStats))
	for k := range stats.TestStats {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var total float64

	for _, k := range keys {
		stat := stats.TestStats[k]
		duration := stat.End.Sub(stat.Start)
		s.T().Logf("%s: %d ms (%.3f s)", k, duration.Milliseconds(), duration.Seconds())
		total += duration.Seconds()
	}

	s.T().Logf("total: %.3f s", total)
}

func TestArangoSuite(t *testing.T) {
	suite.Run(t, new(arangoSuite))
}
