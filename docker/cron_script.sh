#!/usr/bin/env bash

# Error if undefined variables are present:
set -u

# Error if any command in a pipe has a non-zero exit status, not just the last command:
set -o pipefail

# Error if any command returns a non-zero exist status
set -e


# Check for any MRTs the continuous getter missed:
docker run -it --rm \
-v /etc/localtime:/etc/localtime \
-v /media/usb0/:/media/usb0/ \
--name dnas_tmp \
dnas:latest \
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/scripts/get_mrts.py --backfill --yesterday --update --enabled

# Parse any missing MRTs that were downloaded:
docker run -it --rm \
-v /etc/localtime:/etc/localtime \
-v /media/usb0/:/media/usb0/ \
--name dnas_tmp \
dnas:latest \
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/scripts/parse_mrts.py --yesterday --update --remove --enabled

# Generate the daily stats for yesterday:
docker run -it --rm \
-v /etc/localtime:/etc/localtime \
-v /media/usb0/:/media/usb0/ \
--name dnas_tmp \
dnas:latest \
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/scripts/stats.py --yesterday --update --enabled

# Generate the text report from the generated stats:
docker run -it --rm \
-v /etc/localtime:/etc/localtime \
-v /media/usb0/:/media/usb0/ \
-e LANG=C.UTF-8 -e LC_ALL=C.UTF-8 \
--name dnas_tmp \
dnas:latest \
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/scripts/git_reports.py --yesterday

# Tweet the report summary and link to full report:
docker run -it --rm \
-v /etc/localtime:/etc/localtime \
-v /media/usb0/:/media/usb0/ \
--name dnas_tmp \
dnas:latest \
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/scripts/tweet.py --yesterday

