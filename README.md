# DB Bench

Attention! The goal of the tests was to discover and measure the specific use-cases. It's not an extensive (and accurate) benchmark.

Tests were performed on an empty database with default config using a computer containing **40** cores and **125G** of RAM. The document and edge structure defined for ArangoDB:

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

| Num | Test                                               | ArangoDB | PostgreSQL | Neo4j   |
| --: | -------------------------------------------------- | -------- | ---------- | ------- |
|   1 | Create 10 entries                                  | 10 ms    | 15 ms      | 195 ms  |
|   2 | Create 100 entries                                 | 84 ms    | 41 ms      | 715 ms  |
|   3 | Create 1000 entries                                | 690 ms   | 334 ms     | 1.414 s |
|   4 | Create 1000 entries (bulk)                         | 45 ms    | 93ms       | 91 ms   |
|   5 | Create 10000 entries (bulk)                        | 401 ms   | 4.2 s      | 63 ms   |
|   6 | ↪ Read 10000 entries (not a query)                 | 10 s     | skipped    | skipped |
|   7 | ↪ Read 10000 entries (not a query, bulk)           | 257 ms   | skipped    | skipped |
|   8 | ↪ Update 10000 entries                             | 12.3 s   | 84.4 s     | 64.1 s  |
|   9 | ↪ Update 10000 entries (bulk)                      | 848 ms   | 4.9 s      | 25.6 s  |
|  10 | ↪ Read 10000 entries (using query)                 | 328 ms   | 3 s        | 318 ms  |
|  11 | Create 10 connected [pairs](#pair)                 | 7 ms     | 9 ms       | 437 ms  |
|  12 | Create 100 connected pairs                         | 19 ms    | 43 ms      | 247 ms  |
|  13 | Create 10000 connected pairs                       | 1 s      | 68.9 s     | 13.7 s  |
|  14 | ↪ Query all neighbours in pair                     | 554 ms   | 23 ms      | 394 ms  |
|  15 | ↪ Query all neighbours in pair (within one year)   | 40 ms    | 10 ms      | 77 ms   |
|  16 | Create chain with 10000 artifacts                  | 773 ms   | 23 s       | N/A     |
|  17 | ↪ Query 10th artifact in [chain](#chain)           | 1 ms     | 15 ms      | N/A     |
|  18 | ↪ Query 100th artifact in chain                    | 2 ms     | 127 ms     | N/A     |
|  19 | ↪ Query 1000th artifact in chain                   | 316 ms   | 1.1 s      | N/A     |
|  20 | ↪ Query 2000th artifact in chain                   | 2.5 s    | 2.2 s      | N/A     |
|  21 | ↪ Query 5000th artifact in chain                   | 40.7 s   | 5.7 s      | N/A     |
|  22 | ↪ Query 7000th artifact in chain                   | 110.7 s  | 7.8 s      | N/A     |
|  23 | ↪ Sum 5000 `item`s in chain                        | 39.9 s   | 5.6 s      | N/A     |
|  24 | Create 100 direct [neighbours](#direct-neighbours) | 18 ms    | 27 ms      | N/A     |
|  25 | Create 1000 direct neighbours                      | 94 ms    | 328 ms     | N/A     |
|  26 | Create 10000 direct neighbours                     | 785 ms   | 28.9 s     | N/A     |
|  27 | ↪ Query all neighbours (sorted by name)            | 354 ms   | 45 ms      | N/A     |


The following tests were performed on a pre-populated database of **1 million** entries:


| Num | Test                                               | ArangoDB | PostgreSQL |
|----:|----------------------------------------------------|----------|------------|
|   1 | Create 10 entries                                  | 9 ms     | 89 ms      |
|   2 | Create 100 entries                                 | 77 ms    | 81 ms      |
|   3 | Create 1000 entries                                | 805 ms   | 417 ms     |
|   4 | Create 1000 entries (bulk)                         | 49 ms    | 133 ms     |
|   5 | Create 10000 entries (bulk)                        | 421 ms   | 4.1 s      |
|   6 | ↪ Read 10000 entries (not a query)                 | 9.5 s    | skipped    |
|   7 | ↪ Read 10000 entries (not a query, bulk)           | 266 ms   | skipped    |
|   8 | ↪ Update 10000 entries                             | 12.2 s   | 90 s       |
|   9 | ↪ Update 10000 entries (bulk)                      | 983 ms   | 4.8 s      |
|  10 | ↪ Read 10000 entries (using query)                 | 331 ms   | 3 s        |
|  11 | Create 10 connected [pairs](#pair)                 | 5 ms     | 45 ms      |
|  12 | Create 100 connected pairs                         | 16 ms    | 72 ms      |
|  13 | Create 10000 connected pairs                       | 1 s      | 50.2 s     |
|  14 | ↪ Query all neighbours in pair                     | 4.1 s    | 51 ms      |
|  15 | ↪ Query all neighbours in pair (within one year)   | 4.4 s    | 411 ms     |
|  16 | Create chain with 10000 artifacts                  | 770 ms   | 22.9 s     |
|  17 | ↪ Query 10th artifact in [chain](#chain)           | 1 ms     | 12 ms      |
|  18 | ↪ Query 100th artifact in chain                    | 2 ms     | 115 ms     |
|  19 | ↪ Query 1000th artifact in chain                   | 387 ms   | 1.1 s      |
|  20 | ↪ Query 2000th artifact in chain                   | 2.5 s    | 2.2 s      |
|  21 | ↪ Query 5000th artifact in chain                   | 39.2 s   | 5.9 s      |
|  22 | ↪ Query 7000th artifact in chain                   | 108.1 s  | 8.1 s      |
|  23 | ↪ Sum 5000 `item`s in chain                        | 40.1 s   | 5.9 s      |
|  24 | Create 100 direct [neighbours](#direct-neighbours) | 19 ms    | 53 ms      |
|  25 | Create 1000 direct neighbours                      | 96 ms    | 327 ms     |
|  26 | Create 10000 direct neighbours                     | 825 ms   | 21.5 s     |
|  27 | ↪ Query all neighbours (sorted by name)            | 338 ms   | 71 ms      |

* Tests indented by ↪ depend on previous not-indented test.

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

### Direct neighbours

```ascii

|````|     |````|     |````|
| N2 | <-- | N1 | --> | N3 |
|____|     |____|     |____|
             |
             v
           |````|
           | Nn |
           |____|

N  = Node (document/artifact).
Nn = Nth direct neighbour.
```