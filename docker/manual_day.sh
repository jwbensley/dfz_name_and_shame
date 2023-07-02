#!/usr/bin/env bash

# If there was a problem with the pipeline on a specific day,
# use this script to re-run it for that day:
#./manual_day.sh 20220228

# Error if undefined variables are present:
set -u

# Error if any command in a pipe has a non-zero exit status, not just the last command:
set -o pipefail

# Error if any command returns a non-zero exist status
set -e

docker-compose run --rm --name tmp_getter dnas_getter -- \
/opt/dnas/scripts/get_mrts.py --backfill --update --enabled -ymd "$1"

docker-compose run --rm --name tmp_parser dnas_parser -- \
/opt/dnas/scripts/parse_mrts.py --update --remove --enabled --ymd "$1"

docker-compose run --rm --name tmp_stats dnas_stats -- \
/opt/dnas/scripts/stats.py --update --enabled --daily --ymd "$1"

docker-compose run --rm --name tmp_report dnas_stats -- \
/opt/dnas/scripts/git_reports.py --generate --publish --ymd "$1"

#docker-compose run --rm --name tmp_tweet dnas_stats -- \
#/opt/dnas/scripts/tweet.py --generate --tweet --ymd "$1"
