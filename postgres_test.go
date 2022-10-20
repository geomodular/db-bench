package db_bench

import (
	"database/sql"
	"github.com/stretchr/testify/suite"
	"testing"

	_ "github.com/lib/pq"
)

const (
	postgresConnStr = "postgres://user:password@localhost:5455/testdb?sslmode=disable"
)

type postgresSuite struct {
	suite.Suite

	db                      *sql.DB
	artifactIDsToCleanNow   []string
	artifactIDsToCleanLater []string
	edgeIDsToCleanNow       []string
	edgeIDsToCleanLater     []string
}

func (s *postgresSuite) SetupSuite() {

	db, err := initPostgres(postgresConnStr)
	s.Require().NoError(err)

	err = createPostgresTestingTables(db)
	s.Require().NoError(err)

	s.db = db
}

func (s *postgresSuite) TearDownTest() {
	if s.edgeIDsToCleanNow != nil {
		err := removeBulkPostgresEdges(s.db, s.edgeIDsToCleanNow)
		s.Require().NoError(err)

		s.edgeIDsToCleanNow = nil
	}

	if s.artifactIDsToCleanNow != nil {
		err := removeBulkPostgresArtifacts(s.db, s.artifactIDsToCleanNow)
		s.Require().NoError(err)

		s.artifactIDsToCleanNow = nil
	}
}

func (s *postgresSuite) TearDownSuite() {
	if s.edgeIDsToCleanLater != nil {
		err := removeBulkPostgresEdges(s.db, s.edgeIDsToCleanLater)
		s.Require().NoError(err)

		s.edgeIDsToCleanLater = nil
	}

	if s.artifactIDsToCleanLater != nil {
		err := removeBulkPostgresArtifacts(s.db, s.artifactIDsToCleanLater)
		s.Require().NoError(err)

		s.artifactIDsToCleanLater = nil
	}

	_ = s.db.Close()
}

func (s *postgresSuite) Test01_Create10() {

	documentCount := 10

	ids, count, err := createPostgresArtifacts(s.db, documentCount)
	s.Require().NoError(err)
	s.EqualValues(documentCount, count)

	s.artifactIDsToCleanNow = ids
}

func (s *postgresSuite) Test02_Create100() {

	documentCount := 100

	ids, count, err := createPostgresArtifacts(s.db, documentCount)
	s.Require().NoError(err)
	s.EqualValues(documentCount, count)

	s.artifactIDsToCleanNow = ids
}

func (s *postgresSuite) Test03_Create1000() {

	documentCount := 1000

	ids, count, err := createPostgresArtifacts(s.db, documentCount)
	s.Require().NoError(err)
	s.EqualValues(documentCount, count)

	s.artifactIDsToCleanNow = ids
}

func (s *postgresSuite) Test04_BulkCreate1000() {

	documentCount := 1000

	ids, count, err := createBulkPostgresArtifacts(s.db, documentCount)
	s.Require().NoError(err)
	s.EqualValues(documentCount, count)

	s.artifactIDsToCleanNow = ids
}

func (s *postgresSuite) Test05_BulkCreate10000() {

	documentCount := 10000

	ids, count, err := createBulkPostgresArtifacts(s.db, documentCount)
	s.Require().NoError(err)
	s.EqualValues(documentCount, count)

	s.artifactIDsToCleanLater = ids
}

func (s *postgresSuite) Test06_Read10000() {
	s.T().Skip("no similar action in API (to Arango)")
}

func (s *postgresSuite) Test07_BulkRead10000() {
	s.T().Skip("no similar action in API (to Arango)")
}

func (s *postgresSuite) Test08_Update10000() {
	for _, id := range s.artifactIDsToCleanLater {
		err := updateOnePostgresArtifact(s.db, id)
		s.Require().NoError(err)
	}
}

func (s *postgresSuite) Test09_BulkUpdate10000() {
	count, err := updateBulkPostgresArtifacts(s.db, s.artifactIDsToCleanLater)
	s.Require().NoError(err)
	s.EqualValues(len(s.artifactIDsToCleanLater), count)
}

func (s *postgresSuite) Test10_QueryRead10000() {
	err := queryReadPostgresArtifacts(s.db, s.artifactIDsToCleanLater)
	s.Require().NoError(err)

	s.artifactIDsToCleanNow = s.artifactIDsToCleanLater
	s.artifactIDsToCleanLater = nil
}

func (s *postgresSuite) Test11_CreateConnectedPairs10() {

	artifactIDs, edgeIDs, artifactCount, edgeCount, err := createPostgresConnectedPairs(s.db, 10)
	s.Require().NoError(err)
	s.EqualValues(20, artifactCount)
	s.EqualValues(10, edgeCount)

	s.artifactIDsToCleanNow = artifactIDs
	s.edgeIDsToCleanNow = edgeIDs
}

func (s *postgresSuite) Test12_CreateConnectedPairs100() {

	artifactIDs, edgeIDs, artifactCount, edgeCount, err := createPostgresConnectedPairs(s.db, 100)
	s.Require().NoError(err)
	s.EqualValues(200, artifactCount)
	s.EqualValues(100, edgeCount)

	s.artifactIDsToCleanNow = artifactIDs
	s.edgeIDsToCleanNow = edgeIDs
}

func (s *postgresSuite) Test13_CreateConnectedPairs10000() {

	artifactIDs, edgeIDs, artifactCount, edgeCount, err := createPostgresConnectedPairs(s.db, 10000)
	s.Require().NoError(err)
	s.EqualValues(20000, artifactCount)
	s.EqualValues(10000, edgeCount)

	s.artifactIDsToCleanLater = artifactIDs
	s.edgeIDsToCleanLater = edgeIDs
}

func (s *postgresSuite) Test14_QueryAllConnectedPairs10000() {

	count, err := queryAllPostgresPairs(s.db)
	s.Require().NoError(err)
	s.EqualValues(10000, count)
}

func (s *postgresSuite) Test15_QueryAllConnectedPairsOneYear10000() {

	count, err := queryAllPostgresPairsOneYear(s.db, 2022)
	s.Require().NoError(err)
	s.EqualValues(365, count)

	s.edgeIDsToCleanNow = s.edgeIDsToCleanLater
	s.artifactIDsToCleanNow = s.artifactIDsToCleanLater
	s.edgeIDsToCleanLater = nil
	s.artifactIDsToCleanLater = nil
}

func (s *postgresSuite) Test16_Chain1x10000() {

	artifactIDs, edgeIDs, artifactCount, edgeCount, err := createPostgresChain(s.db, 10000)
	s.Require().NoError(err)
	s.EqualValues(10000, artifactCount)
	s.EqualValues(9999, edgeCount)

	s.artifactIDsToCleanNow = artifactIDs
	s.edgeIDsToCleanNow = edgeIDs
}

func (s *postgresSuite) Test17_QueryNeighbourInChain10() {
	s.T().Skip("use recursive query evaluation")
}

func (s *postgresSuite) Test18_QueryNeighbourInChain100() {
	s.T().Skip("use recursive query evaluation")
}

func (s *postgresSuite) Test19_QueryNeighbourInChain1000() {
	s.T().Skip("use recursive query evaluation")
}

func (s *postgresSuite) Test20_QueryNeighbourInChain2000() {
	s.T().Skip("use recursive query evaluation")
}

func (s *postgresSuite) Test21_QueryNeighbourInChain5000() {
	s.T().Skip("use recursive query evaluation")
}

func (s *postgresSuite) Test22_QueryNeighbourInChain7000() {
	s.T().Skip("use recursive query evaluation")
}

func (s *postgresSuite) Test23_SumChainItems5000() {
	s.T().Skip("use recursive query evaluation")
}

func (s *postgresSuite) Test24_CreateNeighbours100() {
	artifactIDs, edgeIDs, artifactCount, edgeCount, err := createPostgresNeighbours(s.db, 100)
	s.Require().NoError(err)
	s.EqualValues(100, artifactCount)
	s.EqualValues(99, edgeCount)

	s.artifactIDsToCleanNow = artifactIDs
	s.edgeIDsToCleanNow = edgeIDs
}

func (s *postgresSuite) Test25_CreateNeighbours1000() {
	artifactIDs, edgeIDs, artifactCount, edgeCount, err := createPostgresNeighbours(s.db, 1000)
	s.Require().NoError(err)
	s.EqualValues(1000, artifactCount)
	s.EqualValues(999, edgeCount)

	s.artifactIDsToCleanNow = artifactIDs
	s.edgeIDsToCleanNow = edgeIDs
}

func (s *postgresSuite) Test26_CreateNeighbours10000() {
	artifactIDs, edgeIDs, artifactCount, edgeCount, err := createPostgresNeighbours(s.db, 10000)
	s.Require().NoError(err)
	s.EqualValues(10000, artifactCount)
	s.EqualValues(9999, edgeCount)

	s.artifactIDsToCleanLater = artifactIDs
	s.edgeIDsToCleanLater = edgeIDs
}

func (s *postgresSuite) Test27_QueryArangoSortedNeighbours10000() {
	count, err := queryPostgresSortedNeighbours(s.db, s.artifactIDsToCleanLater[0])
	s.Require().NoError(err)
	s.EqualValues(9999, count)

}

func (s *postgresSuite) HandleStats(suiteName string, stats *suite.SuiteInformation) {
	printStats(s.T(), suiteName, stats)
}

func TestPostgresSuite(t *testing.T) {
	suite.Run(t, new(postgresSuite))
}
