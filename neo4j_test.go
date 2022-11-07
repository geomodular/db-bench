package db_bench

import (
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/stretchr/testify/suite"
)

type neo4jSuite struct {
	suite.Suite

	driver      neo4j.Driver
	session     neo4j.Session
	keepRecords bool
}

func (s *neo4jSuite) SetupSuite() {
	driver, err := neo4j.NewDriver(Neo4jEndpoint, neo4j.BasicAuth(Neo4jUsername, Neo4jPwd, ""))
	s.Require().NoError((err))

	s.session = driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

	s.driver = driver
}

func (s *neo4jSuite) TearDownTest() {
	if !s.keepRecords {
		s.session.Run("MATCH (n: Entity) DETACH DELETE n", make(map[string]interface{}))
	}
}

func (s *neo4jSuite) TearDownSuite() {
	s.session.Close()
	s.driver.Close()
}

func (s *neo4jSuite) HandleStats(suiteName string, stats *suite.SuiteInformation) {
	printStats(s.T(), suiteName, stats)
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
	s.keepRecords = true

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
	// Arrange
	expectedCount := 10000

	// Act & Assert no errors
	for i := 0; i < expectedCount; i++ {
		err := updateOneEntity(s.session, i)
		s.Require().NoError(err)
	}
}

func (s *neo4jSuite) Test09_BulkUpdate10000() {
	// Arrange
	expectedCount := 10000

	// Act
	updated, err := bulkUpdateEntities(s.session, expectedCount)

	// Assert
	s.Require().NoError(err)
	s.EqualValues(expectedCount, updated)
}

func (s *neo4jSuite) Test10_Read10000() {
	// Arrange
	expectedCount := 10000
	s.keepRecords = false

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

func (s *neo4jSuite) Test12_CreateConnectedPairs100() {
	// Arrange
	expectedCount := 100

	// Act
	created, err := createConnectedPairs(s.session, expectedCount)

	// Assert
	s.Require().NoError(err)
	s.Require().Equal(expectedCount, created)
}

func (s *neo4jSuite) Test13_CreateConnectedPairs10000() {
	// Arrange
	expectedCount := 10000
	s.keepRecords = true

	// Act
	created, err := createConnectedPairs(s.session, expectedCount)

	// Assert
	s.Require().NoError(err)
	s.Require().Equal(expectedCount, created)
}

func (s *neo4jSuite) Test14_QueryAllConnectedPairs10000() {
	// Arrange
	expected := 10000

	// Act
	c, err := s.session.Run("MATCH (x:Entity)-[:RELATED]->(y:Entity) RETURN x", map[string]interface{}{})

	// Assert
	s.Require().NoError(err)
	retrieved := readAllFromCursor(c)
	s.Require().Equal(expected, retrieved)
}

func (s *neo4jSuite) Test15_QueryAllConnectedPairsOneYear10000() {
	// Arrange
	s.keepRecords = false
	expected := 365

	// Act
	c, err := s.session.Run("MATCH (x:Entity)-[:RELATED]->(y:Entity) WHERE x.create_time > $lower AND x.create_time < $upper RETURN x", map[string]interface{}{"lower": "2022", "upper": "2023"})

	// Assert
	s.Require().NoError(err)
	retrieved := readAllFromCursor(c)
	s.Require().Equal(expected, retrieved)
}

func TestNeo4jSuite(t *testing.T) {
	suite.Run(t, new(neo4jSuite))
}
