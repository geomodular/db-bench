package db_bench

import (
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/stretchr/testify/suite"
)

type neo4jSuite struct {
	suite.Suite

	driver  neo4j.Driver
	session neo4j.Session
}

func (s *neo4jSuite) SetupSuite() {
	driver, err := neo4j.NewDriver(Neo4jEndpoint, neo4j.BasicAuth(Neo4jUsername, Neo4jPwd, ""))
	s.Require().NoError((err))

	s.session = driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

	s.driver = driver
}

func (s *neo4jSuite) TearDownTest() {
	// s.session.Run("MATCH (n: Entity) DETACH DELETE n", make(map[string]interface{}))
}

func (s *neo4jSuite) TearDownSuite() {
	s.session.Close()
	s.driver.Close()
}

func (s *neo4jSuite) Test01_Create10() {
	// Arrange
	expectedCount := 10

	// Act
	createdCount, err := createEntities(s.session, expectedCount)

	// Assert
	s.Require().NoError(err)
	s.EqualValues(expectedCount, createdCount)
}

func (s *neo4jSuite) Test02_Create100() {
	// Arrange
	expectedCount := 100

	// Act
	createdCount, err := createEntities(s.session, expectedCount)

	// Assert
	s.Require().NoError(err)
	s.EqualValues(expectedCount, createdCount)
}

func (s *neo4jSuite) Test03_Create1000() {
	// Arrange
	expectedCount := 1000

	// Act
	createdCount, err := createEntities(s.session, expectedCount)

	// Assert
	s.Require().NoError(err)
	s.EqualValues(expectedCount, createdCount)
}

func (s *neo4jSuite) Test04_BulkCreate1000() {
	// Arrange
	expectedCount := 1000

	// Act
	createdCount, err := bulkCreateEntities(s.session, expectedCount)

	// Assert
	s.Require().NoError(err)
	s.EqualValues(expectedCount, createdCount)
}

func (s *neo4jSuite) Test05_BulkCreate10000() {
	// Arrange
	expectedCount := 10000

	// Act
	createdCount, err := bulkCreateEntities(s.session, expectedCount)

	// Assert
	s.Require().NoError(err)
	s.EqualValues(expectedCount, createdCount)
}

func (s *neo4jSuite) Test06_Read10000() {
	s.T().Skip("no similar action in API (to Arango)")
}

func (s *neo4jSuite) Test07_BulkRead10000() {
	s.T().Skip("no similar action in API (to Arango)")
}

func (s *neo4jSuite) Test08_Update10000() {
	s.T().Skip("Too slow!")
	// Arrange
	expectedCount := 1000
	_, err := bulkCreateEntities(s.session, expectedCount)
	s.Require().NoError(err)

	// Act & Assert no errors
	for i := 0; i < expectedCount; i++ {
		err := updateOneEntity(s.session, i)
		s.Require().NoError(err)
	}
}

func (s *neo4jSuite) Test09_BulkUpdate10000() {
	s.T().Skip("Too slow!")
	// Arrange
	expectedCount := 10000
	_, err := bulkCreateEntities(s.session, expectedCount)
	s.Require().NoError(err)

	// Act
	updated, err := bulkUpdateEntities(s.session, expectedCount)

	// Assert
	s.Require().NoError(err)
	s.EqualValues(expectedCount, updated)
}

func (s *neo4jSuite) Test10_Read10000() {
	// Arrange
	expectedCount := 10000
	_, err := bulkCreateEntities(s.session, expectedCount)
	s.Require().NoError(err)

	// Act
	retrieved, err := readMultipleEntities(s.session, expectedCount)

	// Assert
	s.Require().NoError(err)
	s.Require().Equal(expectedCount, retrieved)
}

func (s *neo4jSuite) Test11_CreateConnectedPairs10() {
	// Arrange
	expectedCount := 10

	// Act
	created, err := createConnectedPairs(s.session, expectedCount)

	// Assert
	s.Require().NoError(err)
	s.Require().Equal(expectedCount, created)
}

func TestNeo4jSuite(t *testing.T) {
	suite.Run(t, new(neo4jSuite))
}
