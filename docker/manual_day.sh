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

if [[ ${#} -lt 1 ]]
then
  echo "Wrong number of args: ${#}"
  echo "Call the script with at least a ymd value. E.g.,"
  echo "${0} 20230901"
  echo ""
  echo "Any additional arguments are passed to the scripts."
  exit 1
fi

# shellcheck disable=SC1091
source /opt/dnas/venv/bin/activate
cd "/opt/dnas/docker/"

SCRIPTS_DIR="/opt/dnas/dnas/scripts"
YMD="${1}"
shift

docker-compose run --rm --name tmp_getter dnas_getter -- \
"${SCRIPTS_DIR}/get_mrts.py" --backfill --update --enabled --ymd "${YMD}" "${@}"

docker-compose run --rm --name tmp_parser dnas_parser -- \
"${SCRIPTS_DIR}/parse_mrts.py" --update --remove --enabled --ymd "${YMD}" "${@}"

docker-compose run --rm --name tmp_stats dnas_stats -- \
"${SCRIPTS_DIR}/stats.py" --update --enabled --daily --ymd "${YMD}" "${@}"

docker-compose run --rm --name tmp_report dnas_git -- \
"${SCRIPTS_DIR}/git_reports.py" --generate --publish --ymd "${YMD}" "${@}"

#docker-compose run --rm --name tmp_tweet dnas_stats -- \
#"${SCRIPTS_DIR}/tweet.py" --generate --tweet --ymd "${YMD}" "${@}"
