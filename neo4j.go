package db_bench

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type neo4jEntity struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreateTime  time.Time `json:"create_time"`
	Item        int       `json:"item"`
}

type neo4jRelation struct {
	Body string `json:"Body"`
}

func (e neo4jEntity) toStruct() map[string]interface{} {
	res := make(map[string]interface{})
	data, _ := json.Marshal(e)
	json.Unmarshal(data, &res)
	return res
}

func (e neo4jRelation) toStruct() map[string]interface{} {
	res := make(map[string]interface{})
	data, _ := json.Marshal(e)
	json.Unmarshal(data, &res)
	return res
}

func getName(id int) string {
	return fmt.Sprintf("entity-%d", id)
}

func getDescription(id int) string {
	return fmt.Sprintf("description-%d", id)
}

func createEntities(db neo4j.Session, count int) (created int, err error) {
	_, err = db.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		for i := 0; i < count; i++ {
			entity := neo4jEntity{
				Name:        getName(i),
				Description: getDescription(i),
				CreateTime:  time.Now(),
			}
			entry := map[string]interface{}{"entity": entity.toStruct()}
			_, err = tx.Run("CREATE (:Entity $entity)", entry)
			if err != nil {
				return nil, err
			}

			created++
		}

		return nil, nil
	})

	return
}

func bulkCreateEntities(db neo4j.Session, count int) (created int, err error) {
	var entities []neo4jEntity = make([]neo4jEntity, count)
	for i := range entities {
		entities[i] = neo4jEntity{
			Name:        getName(i),
			Description: getDescription(i),
			CreateTime:  time.Now(),
		}
	}

	var entry map[string]interface{} = map[string]interface{}{"batch": entities}
	data, _ := json.Marshal(entry)
	json.Unmarshal(data, &entry)
	res, err := db.Run(
		`WITH $batch as batch UNWIND batch as props
		CREATE (n:Entity)
		SET n += props`,
		entry,
	)
	if err != nil {
		return
	}

	summary, err := res.Consume()
	if err != nil {
		return
	}

	created = summary.Counters().NodesCreated()
	return
}

func readMultipleEntities(db neo4j.Session, count int) (retrieved int, err error) {
	var nameList []string = make([]string, count)
	for i := range nameList {
		nameList[i] = fmt.Sprintf("new-name-%d", i)
	}

	var entry map[string]interface{} = map[string]interface{}{"names": nameList}
	data, _ := json.Marshal(entry)
	json.Unmarshal(data, &entry)
	cursor, err := db.Run(
		`WITH $names as names
		MATCH (e:Entity)
		WHERE e.name IN names
		RETURN properties(e)`,
		entry,
	)
	if err != nil {
		return
	}

	retrieved = readAllFromCursor(cursor)
	return
}

func readAllFromCursor(c neo4j.Result) int {
	retrieved := 0
	for c.Next() {
		record := c.Record()
		_ = record.Values[0]
		retrieved++
	}

	return retrieved
}

func updateOneEntity(db neo4j.Session, id int) error {
	key := getName(id)
	i := rand.Intn(1000)
	params := map[string]interface{}{
		"key":         key,
		"description": fmt.Sprintf("new-description-%d", i),
	}
	_, err := db.Run(`
		MATCH (e:Entity {name: $key})
		SET e.description = $description`,
		params,
	)
	return err
}

func bulkUpdateEntities(db neo4j.Session, count int) (updated int, err error) {
	var updateList []map[string]interface{} = make([]map[string]interface{}, count)
	for i := range updateList {
		key := getName(i)
		updateList[i] = map[string]interface{}{
			"key":         key,
			"name":        fmt.Sprintf("new-name-%d", i),
			"description": fmt.Sprintf("new-description-%d", i),
		}
	}

	params := map[string]interface{}{"params": updateList}
	ret, err := db.Run(`
		WITH $params AS params
		UNWIND params AS p
		MATCH (e:Entity {name: p.key})
		USING INDEX e:Entity(name)
		SET e.name = p.name
		SET e.description = p.description`,
		params,
	)
	if err != nil {
		return
	}

	res, err := ret.Consume()
	updated = res.Counters().PropertiesSet() / 2
	return
}

func createConnectedPair(tx neo4j.Transaction, first int, second int, created time.Time) error {
	entity1 := neo4jEntity{
		Name:        getName(first),
		Description: getDescription(first),
		CreateTime:  created,
	}
	entity2 := neo4jEntity{
		Name:        getName(second),
		Description: getDescription(second),
		CreateTime:  created,
	}
	relation := neo4jRelation{
		Body: fmt.Sprintf("Connection: %d->%d", first, second),
	}
	entry := map[string]interface{}{
		"entity1":  entity1.toStruct(),
		"entity2":  entity2.toStruct(),
		"relation": relation.toStruct(),
	}

	_, err := tx.Run(`
		CREATE (x:Entity $entity1)
		CREATE (y:Entity $entity2)
		CREATE (x)-[:RELATED $relation]->(y)`,
		entry,
	)

	return err
}

func createConnectedPairs(db neo4j.Session, count int) (created int, err error) {
	_, err = db.WriteTransaction(func(tx neo4j.Transaction) (res interface{}, err error) {
		startDate := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		for i := 0; i < count*2; i = i + 2 {
			err = createConnectedPair(tx, i, i+1, startDate)
			if err != nil {
				return
			}

			startDate = startDate.AddDate(0, 0, 1)
			created++
		}

		return
	})

	return
}
