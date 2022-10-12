
.PHONY: test
test:  ## Run tests. (needs a running and clean databases)
	go test ./... -count=1 -v
