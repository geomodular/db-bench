package db_bench

import (
	"context"
	"github.com/arangodb/go-driver"
	"github.com/stretchr/testify/suite"
	"testing"
)

const (
	arangoEndpoint               = "http://localhost:8529"
	arangoDB                     = "testdb"
	arangoDocumentTestCollection = "testdocumentcollection"
	arangoEdgeTestCollection     = "testedgecollection"
	documentCountNotToCycle      = 1000000
)

type arangoSuite struct {
	suite.Suite

	db                       driver.Database
	documentKeysToCleanNow   []string
	documentKeysToCleanLater []string
	edgeKeysToCleanNow       []string
	edgeKeysToCleanLater     []string
	staticDocumentCount      int
}

func (s *arangoSuite) SetupSuite() {

	db, err := initArango(arangoEndpoint, arangoDB)
	s.Require().NoError(err)

	err = createArangoDocumentCollection(db, arangoDocumentTestCollection)
	s.Require().NoError(err)

	err = createArangoEdgeCollection(db, arangoEdgeTestCollection)
	s.Require().NoError(err)

	ctx := context.Background()
	col, err := db.Collection(ctx, arangoDocumentTestCollection)
	s.Require().NoError(err)

	count, err := col.Count(ctx)
	s.Require().NoError(err)

	s.db = db
	s.staticDocumentCount = int(count)
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
	s.EqualValues(documentCount, count-s.staticDocumentCount)

	// Clean up.
	s.documentKeysToCleanNow = keys
}

func (s *arangoSuite) Test02_Create100() {

	documentCount := 100
	ctx := context.Background()

	keys, count, err := createArangoDocuments(ctx, s.db, arangoDocumentTestCollection, documentCount)
	s.Require().NoError(err)
	s.EqualValues(documentCount, count-s.staticDocumentCount)

	// Clean up.
	s.documentKeysToCleanNow = keys
}

func (s *arangoSuite) Test03_Create1000() {

	documentCount := 1000
	ctx := context.Background()

	keys, count, err := createArangoDocuments(ctx, s.db, arangoDocumentTestCollection, documentCount)
	s.Require().NoError(err)
	s.EqualValues(documentCount, count-s.staticDocumentCount)

	// Clean up.
	s.documentKeysToCleanNow = keys
}

func (s *arangoSuite) Test04_BulkCreate1000() {

	documentCount := 1000
	ctx := context.Background()

	keys, count, err := createBulkArangoDocuments(ctx, s.db, arangoDocumentTestCollection, documentCount)
	s.Require().NoError(err)
	s.EqualValues(documentCount, count-s.staticDocumentCount)

	// Clean up.
	s.documentKeysToCleanNow = keys
}

func (s *arangoSuite) Test05_BulkCreate10000() {

	documentCount := 10000
	ctx := context.Background()

	keys, count, err := createBulkArangoDocuments(ctx, s.db, arangoDocumentTestCollection, documentCount)
	s.Require().NoError(err)
	s.EqualValues(documentCount, count-s.staticDocumentCount)

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

	documentKeys, edgeKeys, documentCount, edgeCount, err := createArangoConnectedPairs(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, 10)
	s.Require().NoError(err)
	s.EqualValues(20, documentCount-s.staticDocumentCount)
	s.EqualValues(10, edgeCount)

	s.documentKeysToCleanNow = documentKeys
	s.edgeKeysToCleanNow = edgeKeys
}

func (s *arangoSuite) Test12_CreateConnectedPairs100() {

	ctx := context.Background()

	documentKeys, edgeKeys, documentCount, edgeCount, err := createArangoConnectedPairs(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, 100)
	s.Require().NoError(err)
	s.EqualValues(200, documentCount-s.staticDocumentCount)
	s.EqualValues(100, edgeCount)

	s.documentKeysToCleanNow = documentKeys
	s.edgeKeysToCleanNow = edgeKeys
}

func (s *arangoSuite) Test13_CreateConnectedPairs10000() {

	ctx := context.Background()

	documentKeys, edgeKeys, documentCount, edgeCount, err := createArangoConnectedPairs(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, 10000)
	s.Require().NoError(err)
	s.EqualValues(20000, documentCount-s.staticDocumentCount)
	s.EqualValues(10000, edgeCount)

	s.documentKeysToCleanLater = documentKeys
	s.edgeKeysToCleanLater = edgeKeys
}

func (s *arangoSuite) Test14_QueryAllConnectedPairs10000() {

	if s.staticDocumentCount > documentCountNotToCycle {
		s.T().Skip("too many documents to cycle over")
	}

	ctx := context.Background()

	count, err := queryAllArangoPairs(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection)
	s.Require().NoError(err)
	s.EqualValues(10000, count)
}

func (s *arangoSuite) Test15_QueryAllConnectedPairsOneYear10000() {

	if s.staticDocumentCount > documentCountNotToCycle {
		s.T().Skip("too many documents to cycle over")
	} else {
		ctx := context.Background()

		count, err := queryAllArangoPairsOneYear(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, 2022)
		s.Require().NoError(err)
		s.EqualValues(365, count)
	}

	s.documentKeysToCleanNow = s.documentKeysToCleanLater
	s.edgeKeysToCleanNow = s.edgeKeysToCleanLater
	s.documentKeysToCleanLater = nil
	s.edgeKeysToCleanLater = nil
}

func (s *arangoSuite) Test16_CreateChain1x10000() {
	ctx := context.Background()

	documentKeys, edgeKeys, documentCount, edgeCount, err := createArangoChain(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, 10000, 1)
	s.Require().NoError(err)
	s.EqualValues(10000, documentCount-s.staticDocumentCount)
	s.EqualValues(9999, edgeCount)

	s.documentKeysToCleanLater = documentKeys
	s.edgeKeysToCleanLater = edgeKeys
}

func (s *arangoSuite) Test17_QueryNeighbourInChain10() {
	ctx := context.Background()

	document, err := queryArangoNeighbourN(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, s.documentKeysToCleanLater[0], 10)
	s.Require().NoError(err)
	s.EqualValues("artifact-10", document.Name)
	s.EqualValues(s.documentKeysToCleanLater[10], document.Key)
}

func (s *arangoSuite) Test18_QueryNeighbourInChain100() {
	ctx := context.Background()

	document, err := queryArangoNeighbourN(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, s.documentKeysToCleanLater[0], 100)
	s.Require().NoError(err)
	s.EqualValues("artifact-100", document.Name)
	s.EqualValues(s.documentKeysToCleanLater[100], document.Key)
}

func (s *arangoSuite) Test19_QueryNeighbourInChain1000() {
	ctx := context.Background()

	document, err := queryArangoNeighbourN(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, s.documentKeysToCleanLater[0], 1000)
	s.Require().NoError(err)
	s.EqualValues("artifact-1000", document.Name)
	s.EqualValues(s.documentKeysToCleanLater[1000], document.Key)
}

func (s *arangoSuite) Test20_QueryNeighbourInChain2000() {
	ctx := context.Background()

	document, err := queryArangoNeighbourN(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, s.documentKeysToCleanLater[0], 2000)
	s.Require().NoError(err)
	s.EqualValues("artifact-2000", document.Name)
	s.EqualValues(s.documentKeysToCleanLater[2000], document.Key)
}

func (s *arangoSuite) Test21_QueryNeighbourInChain5000() {
	ctx := context.Background()

	document, err := queryArangoNeighbourN(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, s.documentKeysToCleanLater[0], 5000)
	s.Require().NoError(err)
	s.EqualValues("artifact-5000", document.Name)
	s.EqualValues(s.documentKeysToCleanLater[5000], document.Key)
}

func (s *arangoSuite) Test22_QueryNeighbourInChain7000() {
	ctx := context.Background()

	document, err := queryArangoNeighbourN(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, s.documentKeysToCleanLater[0], 7000)
	s.Require().NoError(err)
	s.EqualValues("artifact-7000", document.Name)
	s.EqualValues(s.documentKeysToCleanLater[7000], document.Key)
}

func (s *arangoSuite) Test23_SumChainItems5000() {
	ctx := context.Background()

	sum, err := sumArangoNeighbourNItems(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, s.documentKeysToCleanLater[0], 4999)
	s.Require().NoError(err)
	s.EqualValues(5000, sum)

	s.documentKeysToCleanNow = s.documentKeysToCleanLater
	s.edgeKeysToCleanNow = s.edgeKeysToCleanLater
	s.documentKeysToCleanLater = nil
	s.edgeKeysToCleanLater = nil
}

func (s *arangoSuite) Test24_CreateNeighbours100() {
	ctx := context.Background()

	documentKeys, edgeKeys, documentCount, edgeCount, err := createArangoNeighbours(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, 100)
	s.Require().NoError(err)
	s.EqualValues(100, documentCount-s.staticDocumentCount)
	s.EqualValues(99, edgeCount)

	s.documentKeysToCleanNow = documentKeys
	s.edgeKeysToCleanNow = edgeKeys
}

func (s *arangoSuite) Test25_CreateNeighbours1000() {
	ctx := context.Background()

	documentKeys, edgeKeys, documentCount, edgeCount, err := createArangoNeighbours(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, 1000)
	s.Require().NoError(err)
	s.EqualValues(1000, documentCount-s.staticDocumentCount)
	s.EqualValues(999, edgeCount)

	s.documentKeysToCleanNow = documentKeys
	s.edgeKeysToCleanNow = edgeKeys
}

func (s *arangoSuite) Test26_CreateNeighbours10000() {
	ctx := context.Background()

	documentKeys, edgeKeys, documentCount, edgeCount, err := createArangoNeighbours(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, 10000)
	s.Require().NoError(err)
	s.EqualValues(10000, documentCount-s.staticDocumentCount)
	s.EqualValues(9999, edgeCount)

	s.documentKeysToCleanLater = documentKeys
	s.edgeKeysToCleanLater = edgeKeys
}

func (s *arangoSuite) Test27_QueryArangoSortedNeighbours10000() {

	ctx := context.Background()

	count, err := queryArangoSortedNeighbours(ctx, s.db, arangoDocumentTestCollection, arangoEdgeTestCollection, s.documentKeysToCleanLater[0])
	s.Require().NoError(err)
	s.EqualValues(9999, count)
}

func (s *arangoSuite) HandleStats(suiteName string, stats *suite.SuiteInformation) {
	printStats(s.T(), suiteName, stats)
}

func TestArangoSuite(t *testing.T) {
	suite.Run(t, new(arangoSuite))
}
