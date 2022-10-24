#!/bin/bash

docker volume create pgdata
docker run  \
    --name postgresDB \
    -v pgdata:/var/lib/postgresql/data \
    --rm \
    -p 5455:5432 \
    -e POSTGRES_USER=user \
    -e POSTGRES_PASSWORD=password \
    -e POSTGRES_DB=testdb \
    postgres
