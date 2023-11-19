#!/bin/bash -x

set -eu

# shellcheck disable=SC1091
source /opt/dnas/venv/bin/activate

datetime=$(date "+%Y-%m-%d") # 2023-01-01
backup_dir="/opt/dnas/redis/backups"
backup_file="${backup_dir}/redis-${datetime}.json"

mkdir -p "${backup_dir}"
chown bensley:bensley "${backup_dir}"
cd /opt/dnas/docker || exit 1
docker-compose run --rm --name redis_backup dnas_parser -- /opt/dnas/dnas/scripts/redis_mgmt.py --stream --dump "${backup_file}"
echo "Uncompressed size is: $(ls -lh "${backup_file}")"
gzip "${backup_file}"
echo "Compressed size is: $(ls -lh "${backup_file}.gz")"
