#!/bin/bash -x

set -eu

datetime=$(date "+%Y-%m-%d")
backup_dir="/opt/dnas/redis/backups"
backup_file="${backup_dir}/redis-${datetime}.json"

mkdir -p "${backup_dir}"
chown bensley:bensley "${backup_dir}"
source /opt/dnas/venv/bin/activate
cd /opt/dnas/dnas/scripts/ || exit 1
./redis_mgmt.py --dump "${backup_file}"
gzip "${backup_file}"

