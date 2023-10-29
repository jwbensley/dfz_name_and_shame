#!/usr/bin/env bash

set -e

cd /opt/dnas/
# shellcheck disable=SC1091
source venv/bin/activate
git checkout main
git pull origin main --rebase
cd docker/
# Ensure we have latest version of base images
docker pull ubuntu:22.04
docker pull redis:6.2.6
docker-compose build
docker images -f dangling=true
docker image prune -f
