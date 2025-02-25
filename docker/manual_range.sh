#!/usr/bin/env bash

# If there was a problem with the pipeline for multiple days,
# use this script to re-run it for those days:
# start y/m/d end y/m/d
#./manual_day.sh 2023 09 01 2023 09 30

# Error if undefined variables are present:
set -u

# Error if any command in a pipe has a non-zero exit status, not just the last command:
set -o pipefail

# Error if any command returns a non-zero exist status
set -e

if [[ ${#} -lt 6 ]]
then
  echo "Wrong number of args: ${#}"
  echo "Call the script with at least 6 values, all space separated."
  echo "Start year, start month, start day, end year, end month, end day."
  echo "These should have leading zeros e.g.,"
  echo "${0} 2023 09 01 2023 09 30"
  echo ""
  echo "Any additional arguments are passed to the scripts."
  exit 1
fi

# shellcheck disable=SC1091
source /opt/dnas/venv/bin/activate
cd "/opt/dnas/docker/"

PYPY="/opt/pypy"
SCRIPTS_DIR="/opt/dnas/dnas/scripts"
SY="${1}"
shift
SM="${1}"
shift
SD="${1}"
shift
EY="${1}"
shift
EM="${1}"
shift
ED="${1}"
shift

for year in $(seq -w "$SY" "$EY")
do
  for month in $(seq -w "$SM" "$EM")
  do
    for day in $(seq -w "$SD" "$ED")
    do
      echo "doing ${year}${month}${day}:"
      docker compose run --rm --name tmp_getter_range --entrypoint "${PYPY}" dnas_getter -- \
      "${SCRIPTS_DIR}/get_mrts.py" \
      --backfill --update --enabled --ymd "${year}${month}${day}" "${@}"

      docker compose run --rm --name tmp_parser_range --entrypoint "${PYPY}" dnas_parser -- \
      "${SCRIPTS_DIR}/parse_mrts.py" \
      --update --remove --enabled --ymd "${year}${month}${day}" "${@}"

      docker compose run --rm --name tmp_stats_range --entrypoint "${PYPY}" dnas_stats -- \
      "${SCRIPTS_DIR}/stats.py" \
      --update --enabled --daily --ymd "${year}${month}${day}" "${@}"

      docker compose run --rm --name tmp_git_range --entrypoint "${PYPY}" dnas_stats -- \
      "${SCRIPTS_DIR}/git_reports.py" \
      --generate --publish --ymd "${year}${month}${day}" "${@}"

      #docker compose run --rm --name tmp_tweet_range --entrypoint "${PYPY}" dnas_stats -- \
      #"${SCRIPTS_DIR}/tweet.py" \
      #--generate --tweet --ymd "${year}${month}${day}" "${@}"

    done
  done
done

