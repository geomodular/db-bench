#!/bin/bash

docker run  \
    --name postgresDB \
    --rm \
    -p 5455:5432 \
    -e POSTGRES_USER=user \
    -e POSTGRES_PASSWORD=password \
    -e POSTGRES_DB=testdb \
    postgres