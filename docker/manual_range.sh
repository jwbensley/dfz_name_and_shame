#!/usr/bin/env bash

# If there was a problem with the pipeline on range of days,
# use this script to re-run it for those days:
# start y/m/d end y/m/d
#./manual_day.sh 2023 09 01 2023 09 30

# Error if undefined variables are present:
set -u

# Error if any command in a pipe has a non-zero exit status, not just the last command:
set -o pipefail

# Error if any command returns a non-zero exist status
set -e

if [[ ${#} -ne 6 ]]
then
  echo "Wrong number of args ${#}."
  echo "Call the script with 6 values all space seperated."
  echo "Start year, month, day, end year, month, day."
  echo "These should have leading zeros e.g.,"
  echo "${0} 2023 09 01 2023 09 30"
  exit 1
fi

SCRIPTS="/opt/dnas/dnas/scripts"
SY="${1}"
SM="${2}"
SD="${3}"
EY="${4}"
EM="${5}"
ED="${6}"

for year in $(seq -w $SY $EY)
do
  for month in $(seq -w $SM $EM)
  do
    for day in $(seq -w $SD $ED)
    do
      echo "doing ${year}${month}${day}:"
      docker-compose run --rm --name tmp_getter --entrypoint /opt/pypy dnas_getter -- \
      "${SCRIPTS}/get_mrts.py" \
      --backfill --update --enabled --ymd "${year}${month}${day}"
    done
  done
done

for year in $(seq -w "$SY" "$EY")
do
  for month in $(seq -w "$SM" "$EM")
  do
    for day in $(seq -w "$SD" "$ED")
    do
      echo "doing ${year}${month}${day}:"
      docker-compose run --rm --name tmp_parser --entrypoint /opt/pypy dnas_parser -- \
      "${SCRIPTS}/parse_mrts.py" \
      --update --remove --enabled --no-multi --ymd "${year}${month}${day}"
    done
  done
done

for year in $(seq -w "$SY" "$EY")
do
  for month in $(seq -w "$SM" "$EM")
  do
    for day in $(seq -w "$SD" "$ED")
    do
      echo "doing ${year}${month}${day}:"
      docker-compose run --rm --name tmp_stats --entrypoint /opt/pypy dnas_stats -- \
      "${SCRIPTS}/stats.py" \
      --update --enabled --daily --ymd "${year}${month}${day}"
    done
  done
done

for year in $(seq -w "$SY" "$EY")
do
  for month in $(seq -w "$SM" "$EM")
  do
    for day in $(seq -w "$SD" "$ED")
    do
      echo "doing ${year}${month}${day}:"
      docker-compose run --rm --name tmp_git --entrypoint /opt/pypy dnas_stats -- \
      "${SCRIPTS}/git_reports.py" \
      --generate --publish --ymd "${year}${month}${day}"
    done
  done
done

#docker-compose run --rm --name tmp_tweet dnas_stats -- \
#"${SCRIPTS}/tweet.py" --generate --tweet --ymd "$1"
