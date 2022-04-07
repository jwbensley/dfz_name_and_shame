#!/usr/bin/env bash

# Error if undefined variables are present:
set -u

# Error if any command in a pipe has a non-zero exit status, not just the last command:
set -o pipefail

# Error if any command returns a non-zero exist status
set -e


# Check for any MRTs the continuous getter missed:
docker run -it --rm -v /etc/localtime:/etc/localtime -v /media/usb0/:/media/usb0/ -v /opt/dnas/secrets/redis_auth.py:/opt/dnas/dnas/dnas/redis_auth.py --name dnas_tmp dnas:latest /opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/dnas/scripts/get_mrts.py --yesterday --update

# Parse any missing MRTs that were downloaded:
docker run -it --rm -v /etc/localtime:/etc/localtime -v /media/usb0/:/media/usb0/ -v /opt/dnas/secrets/redis_auth.py:/opt/dnas/dnas/dnas/redis_auth.py --name dnas_tmp dnas:latest /opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/dnas/scripts/parse_mrts.py --yesterday --update --remove

# Generate the daily stats for yesterday:
docker run -it --rm -v /etc/localtime:/etc/localtime -v /media/usb0/:/media/usb0/ -v /opt/dnas/secrets/redis_auth.py:/opt/dnas/dnas/dnas/redis_auth.py --name dnas_tmp dnas:latest /opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/dnas/scripts/stats.py --yesterday --update

# Generate the text report from the generated stats:
docker run -it --rm -v /etc/localtime:/etc/localtime -v /media/usb0/:/media/usb0/ -v /opt/dnas/secrets/redis_auth.py:/opt/dnas/dnas/dnas/redis_auth.py -v /opt/dnas/secrets/ed25519:/root/.ssh/id_ed25519 -v /opt/dnas/secrets/ed25519.pub:/root/.ssh/id_ed25519.pub -e LANG=C.UTF-8 -e LC_ALL=C.UTF-8 --name dnas_tmp dnas:latest /opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/dnas/scripts/git_reports.py --yesterday

# Tweet the report summary and link to full report:
docker run -it --rm -v /etc/localtime:/etc/localtime -v /media/usb0/:/media/usb0/ -v /opt/dnas/secrets/redis_auth.py:/opt/dnas/dnas/dnas/redis_auth.py -v /opt/dnas/secrets/twitter_auth.py:/opt/dnas/dnas/dnas/twitter_auth.py --name dnas_tmp dnas:latest /opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/dnas/scripts/tweet.py --yesterday
