#!/bin/bash

docker volume create n4jdata
docker run \
    --publish=7474:7474 --publish=7687:7687 \
    -v n4jdata:/data \
    neo4j