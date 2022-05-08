#!/bin/bash

cd /opt/dnas/
git pull
docker build -t dnas:latest -f docker/dnas.Dockerfile .
