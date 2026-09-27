#!/bin/bash

set -eu

# shellcheck disable=SC1091
source /opt/dnas/venv/bin/activate

datetime=$(date "+%Y-%m-%d") # 2023-01-01
backup_dir="/opt/dnas/redis/backups"
backup_file="${backup_dir}/redis-${datetime}.json"
min_backup_count=20

# Delete old backups first, to free up space but maintain a minimum number of backups
# shellcheck disable=SC2061
for file in $(find "$backup_dir" -name *.json.gz | sort | head -n -"$min_backup_count")
do
  echo "Deleting $file"
  rm "$file"
done

# shellcheck disable=SC2061
echo "Remaining backups: $(find "$backup_dir" -name *.json.gz | wc -l)"
# shellcheck disable=SC2061
find "$backup_dir" -name *.json.gz | sort

mkdir -p "${backup_dir}"
chown bensley:bensley "${backup_dir}"
cd /opt/dnas/docker || exit 1
docker compose run --rm --name redis_backup dnas_parser -- /opt/dnas/dnas/scripts/redis_mgmt.py --stream --dump "${backup_file}"
echo "Uncompressed size is: $(ls -lh "${backup_file}")"
echo "Free space:"
df -h
gzip "${backup_file}"
echo "Compressed size is: $(ls -lh "${backup_file}.gz")"

# shellcheck disable=SC2061
backup_count=$(find "$backup_dir" -name *.json.gz | wc -l)
echo "Backup count: $backup_count"
if [ "$backup_count" -lt "$min_backup_count" ]
then
    echo "Backup count is low!"
fi
