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

	db                    *sql.DB
	artifactIDsToCleanNow []string
}

func (s *postgresSuite) SetupSuite() {

	db, err := initPostgres(postgresConnStr)
	s.Require().NoError(err)

	err = createPostgresTestingTables(db)
	s.Require().NoError(err)

	s.db = db
}

func (s *postgresSuite) TearDownTest() {
	if s.artifactIDsToCleanNow != nil {
		err := removeBulkPostgresArtifacts(s.db, s.artifactIDsToCleanNow)
		s.Require().NoError(err)

		s.artifactIDsToCleanNow = nil
	}
}

func (s *postgresSuite) TearDownSuite() {
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

	s.artifactIDsToCleanNow = ids
}

func (s *postgresSuite) HandleStats(suiteName string, stats *suite.SuiteInformation) {
	printStats(s.T(), suiteName, stats)
}

func TestPostgresSuite(t *testing.T) {
	suite.Run(t, new(postgresSuite))
}
