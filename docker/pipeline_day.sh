#!/usr/bin/env bash

# If there was a problem with the pipeline on a specific day,
# use this script to re-run it for that day:
#./pipeline_day.sh 20220228

# Error if undefined variables are present:
set -u

# Error if any command in a pipe has a non-zero exit status, not just the last command:
set -o pipefail

# Error if any command returns a non-zero exist status
set -e

pypy="/opt/pypy3.8-v7.3.7-linux64/bin/pypy3"

source /opt/dnas/venv/bin/active

docker-compose run --rm \
--name "dnas_tmp" \
--entrypoint "${pypy}" \
dnas_getter \
/opt/dnas/scripts/get_mrts.py \
--backfill --update --enabled --range --start "${1}".0000 --end "${1}".2359

#docker-compose run --rm \
#--name "dnas_tmp" \
#--entrypoint "${pypy}" \
#dnas_parser \
#/opt/dnas/scripts/parse_mrts.py \
#--update --remove --enabled --ymd "${1}"

