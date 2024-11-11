#!/usr/bin/env bash

set -e

DATA_DIR="/opt/dnas_data/"
APP_DIR="/opt/dnas/"

mkdir -p "$DATA_DIR"
mkdir -p "$APP_DIR"
cd "$APP_DIR"

if [ ! -d ".git/" ]
then
    git clone git@github.com:jwbensley/dfz_name_and_shame.git .
else
    git checkout main
    git pull origin main --rebase
fi

cd docker/
# Ensure latest images are present
docker compose pull --ignore-buildable
docker compose build
# Clean up old images
docker images -f dangling=true
docker image prune -f
