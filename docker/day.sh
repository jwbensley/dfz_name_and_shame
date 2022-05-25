#!/bin/bash

# If there was a problem with the pipeline on a specific day,
# use this script to re-run it for that day:
#./day.sh 20220228

# Error if undefined variables are present:
set -u

# Error if any command in a pipe has a non-zero exit status, not just the last command:
set -o pipefail

# Error if any command returns a non-zero exist status
set -e

#pypy="/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3"
pypy="/opt/pypy3.8-v7.3.7-linux64/bin/pypy3"

docker run -it --rm \
-v /etc/localtime:/etc/localtime \
-v /media/usb0/:/media/usb0/ \
--name dnas_tmp \
dnas:latest \
"$pypy" /opt/dnas/scripts/get_mrts.py --backfill --update --enabled --range --start "$1".0000 --end "$1".2359

docker run -it --rm \
-v /etc/localtime:/etc/localtime \
-v /media/usb0/:/media/usb0/ \
--name dnas_tmp \
dnas:latest \
"$pypy" /opt/dnas/scripts/parse_mrts.py --update --remove --enabled --ymd "$1"

docker run -it --rm \
-v /etc/localtime:/etc/localtime \
-v /media/usb0/:/media/usb0/ \
--name dnas_tmp \
dnas:latest \
"$pypy" /opt/dnas/scripts/stats.py --update --enabled --daily --ymd "$1"

docker run -it --rm \
-v /etc/localtime:/etc/localtime \
-v /media/usb0/:/media/usb0/ \
-e LANG=C.UTF-8 -e LC_ALL=C.UTF-8 \
--name dnas_tmp \
dnas:latest \
"$pypy" /opt/dnas/scripts/git_reports.py --generate --publish --ymd "$1"

docker run -it --rm \
-v /etc/localtime:/etc/localtime \
-v /media/usb0/:/media/usb0/ \
--name dnas_tmp \
dnas:latest \
"$pypy" /opt/dnas/scripts/tweet.py --generate --tweet --ymd "$1"
