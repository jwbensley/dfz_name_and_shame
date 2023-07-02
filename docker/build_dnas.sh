#!/bin/bash

set -e

cd /opt/dnas/
git pull
cd docker/
docker-compose build
docker images -f dangling=true
docker image prune -f
