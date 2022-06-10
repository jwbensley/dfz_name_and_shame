#!/bin/bash

cd /opt/dnas/
git pull
docker build -t dnas:latest -f docker/dnas.Dockerfile .
docker images -f dangling=true
docker image prune -f

