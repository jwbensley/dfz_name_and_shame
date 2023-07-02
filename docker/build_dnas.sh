#!/bin/bash

set -e

cd /opt/dnas/
source venv/bin/activate
git checkout main
git pull
cd docker/
# Ensure we have latest version of base images
docker pull ubuntu:22.04
docker pull redis:6.2.6
docker-compose build
docker images -f dangling=true
docker image prune -f
