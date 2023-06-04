#!/bin/bash

cd /opt/dnas/
git pull
cd docker/
docker-compuse build
docker images -f dangling=true
docker image prune -f

