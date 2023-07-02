#!/bin/bash

set -e

cd /opt/dnas/
git checkout main
git pull
cd docker/
docker-compose build
docker images -f dangling=true
docker image prune -f
