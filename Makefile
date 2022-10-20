
.PHONY: test tests test-arango test-postgres

test tests:  ## Run tests. (needs a running and clean databases)
	go test ./... -count=1 -v -timeout 30m

test-arango:
	go test  ./... -count=1 -v -timeout 30m -run TestArangoSuite

test-postgres:
	go test  ./... -count=1 -v -timeout 30m -run TestPostgresSuite


