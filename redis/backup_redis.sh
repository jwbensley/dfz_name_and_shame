#!/bin/bash

set -eu

# shellcheck disable=SC1091
source /opt/dnas/venv/bin/activate

datetime=$(date "+%Y-%m-%d") # 2023-01-01
backup_dir="/opt/dnas/redis/backups"
backup_file="${backup_dir}/redis-${datetime}.json"

mkdir -p "${backup_dir}"
chown bensley:bensley "${backup_dir}"
cd /opt/dnas/docker || exit 1
docker compose run --rm --name redis_backup dnas_parser -- /opt/dnas/dnas/scripts/redis_mgmt.py --stream --dump "${backup_file}"
echo "Uncompressed size is: $(ls -lh "${backup_file}")"
echo "Free space:"
df -h
gzip "${backup_file}"
echo "Compressed size is: $(ls -lh "${backup_file}.gz")"

backup_count=$(find "$backup_dir" -name *.json.gz | wc -l)
echo "Backup count: $backup_count"
if [ "$backup_count" -lt 26 ]
then
    echo "Backup count is low, exiting"
    exit 0
fi

for file in $(find "$backup_dir" -name *.json.gz | sort | head -n -10)
do
  echo "Deleting $file"
  rm "$file"
done

echo "Remaining backups: $(find "$backup_dir" -name *.json.gz | wc -l)"
find "$backup_dir" -name *.json.gz | sort

