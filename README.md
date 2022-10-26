
Tests was performed on an empty database with default config using a computer containing **40** cores and **125G** of RAM. The document and edge structure defined for ArangoDB:

```go
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
```

The corresponding tables to simulate on using PostgreSQL:

```sql
CREATE TABLE IF NOT EXISTS artifacts
(
    id           UUID PRIMARY KEY,
    "name"       TEXT NOT NULL,
    description  TEXT,
    item         INTEGER DEFAULT 1,
    create_time  TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS edges
(
    id      UUID PRIMARY KEY,
    "from"  UUID REFERENCES artifacts,
    "to"    UUID REFERENCES artifacts,
    body    TEXT
)
```

| Num | Test                                           | ArangoDB | PostgreSQL |
|----:|------------------------------------------------|----------|------------|
|   1 | Create 10 entries                              | 10 ms    |            |
|   2 | Create 100 entries                             | 84 ms    |            |
|   3 | Create 1000 entries                            | 690 ms   |            |
|   4 | Create 1000 entries (bulk)                     | 45 ms    |            |
|   5 | Create 10000 entries (bulk)                    | 401 ms   |            |
|   6 | Read 10000 entries (not a query)               | 10 s     |            |
|   7 | Read 10000 entries (not a query, bulk)         | 257 ms   |            |
|   8 | Update 10000 entries (not a query)             | 12.3 s   |            |
|   9 | Update 10000 entries (not a query, bulk)       | 848 ms   |            |
|  10 | Read 10000 entries (using query)               | 328 ms   |            |
|  11 | Create 10 connected [pairs](#Pair)             | 7 ms     |            |
|  12 | Create 100 connected pairs                     | 19 ms    |            |
|  13 | Create 10000 connected pairs                   | 1 s      |            |
|  14 | Query all neighbours in pair                   | 554 ms   |            |
|  15 | Query all neighbours in pair (within one year) | 40 ms    |            |
|  16 | Create chain with 10000 artifacts              | 773 ms   |            |
|  17 | Query 10th artifact in [chain](#Chain)         | 1 ms     |            |
|  18 | Query 100th artifact in chain                  | 2 ms     |            |
|  19 | Query 1000th artifact in chain                 | 316 ms   |            |
|  20 | Query 2000th artifact in chain                 | 2.5 s    |            |
|  21 | Query 5000th artifact in chain                 | 40.7 s   |            |
|  22 | Query 7000th artifact in chain                 | 110.7 s  |            |
|  23 | Sum 5000 `item`s in chain                      | 39.9 s   |            |
|  24 | Create 100 direct neighbours                   | 18 ms    |            |
|  25 | Create 1000 direct neighbours                  | 94 ms    |            |
|  26 | Create 10000 direct neighbours                 | 785 ms   |            |
|  27 | Query all neighbours (sorted by name)          | 354 ms   |            |


### Pair

```ascii

|````|     |````|
| N1 | --> | N2 |
|____|     |____|

N  = Node (document/artifact).
N2 = Neighbour node to N1.
```

### Chain

```ascii

|````|     |````|           |````|
| N1 | --> | N2 | -> ... -> | Nn |
|____|     |____|           |____|

N  = Node (document/artifact).
Nn = Nth node in a chain.
```