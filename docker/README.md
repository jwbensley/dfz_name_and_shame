# DNAS Docker Details

## Building

To build the Redis DB container and DNAS containers use the following:

```bash
cd /opt/dnas/
source venv/bin/activate
cd docker/
docker-compose build
```

One can run `docker-compose build` to rebuild the Redis and DNAS containers any time. However this doesn't pull the latest software version from Git. To pull the latest code from the Git repo and rebuild the DNAS container run the build script: `/opt/dnas/docker/build_dnas.sh`

### Build Issues

If you see output like the following:
```shell
docker-compose build
...
Ign:1 http://mirror.mythic-beasts.com/ubuntu focal InRelease
Ign:2 http://mirror.mythic-beasts.com/ubuntu focal-updates InRelease
Ign:3 http://mirror.mythic-beasts.com/ubuntu focal-backports InRelease
Ign:4 http://mirror.mythic-beasts.com/ubuntu focal-security InRelease
Ign:5 http://mirror.mythic-beasts.com/mythic mythic InRelease
```

Update docker on the host machine (`sudo apt-get update && sudo apt-get --no-install-recommends upgrade`)

## Running

### In Pipeline Mode

Use docker-compose to start the Redis container, and the MRT-getter and MRT-parser containers in continuous mode:

```bash
cd /opt/dnas/
source venv/bin/activate
cd docker/
docker-compose up -d
docker-compose logs -f
```

To shut the entire pipeline down simply run: `docker-compose down`

On Ubuntu the `ufw` may block connections between containers.  

* This will allow REDIS access from the other contains: `sudo ufw allow from 172.16.0.0/12 to 172.16.0.0/12 proto tcp port 6379` (TODO: this should be locked down further. Also, is it even needed now that DNAS is migrated from `docker` to `docker-compose` ?)
* This will allow access to BIRD: `sudo ufw allow from 172.16.0.0/12 to 172.16.0.0/12 proto tcp port 8000` (TODO: this should be locked down further. Also, is it even needed now that DNAS is migrated from `docker` to `docker-compose` ?)

To start an individual container from the pipeline use: `docker-compose up -d dnas_parser`

To stop and remove an individual container use: `docker-compose stop dnas_parser && docker-compose rm dnas_parser`

To run BASH on a container use: `docker-compose exec dnas_parser /bin/bash`
&nbsp;

#### Redis

Redis uses authentication so you won't be able to access the redis CLI without authenticating first. With the container running in the background, start an interactive redis-cli process on the same container (`docker-compose exec dnas_redis redis-cli`). Use the auth command in the Redis CLI `AUTH xxxxx` (password from redis.conf file) and test with command `PING`.  
&nbsp;

#### Continuous MRT Getter

The local time configuration file from the host is shared into the container because the host is not in UTC0/GMT time, but most MRT dumps use UTC0 timestamps.

### In Retrospective Mode

Pull any missing MRTs for a specific day:
```shell
docker-compose run --rm --name tmp_getter dnas_getter -- \
/opt/dnas/dnas/scripts/get_mrts.py --backfill --update --enabled --ymd "20230101"
```

Run the parser for a specfic file:
```shell
docker-compose run --rm --name tmp_parser dnas_parser -- \
/opt/dnas/dnas/scripts/parse_mrts.py --debug --remove --single /opt/dnas_data/downloads/SYDNEY/updates.20230424.0615.bz2
```

Run the parser for a specific time range (this can be less than a day or longer than a day, also note that this expects all MRTs not currently in the DB to already exist on disk, it will fail if some are missing):
```shell
docker-compose run --rm --name tmp_parser dnas_parser -- \
/opt/dnas/dnas/scripts/parse_mrts.py --update --remove --enabled --start "20230101.0000" --end "20230101.2359"
```

Run an the parser for a specific day (note that this will try to pass all MRTs that exist on disk for the specified day, and will not fail if some are missing from disk which are also missing in the DB):
```shell
docker-compose run --rm --name tmp_parser dnas_parser -- \
/opt/dnas/dnas/scripts/parse_mrts.py --update --remove --enabled --ymd "20230101"
```

Parse multiple days and don't worry about missing MRTs:
```shell
for year in {2023..2023}
do
  for month in {02..02}
  do
    for day in {01..28}
    do
      echo "doing ${year}${month}${day}:"
      docker-compose run --rm --name tmp_parser dnas_parser -- \
      /opt/dnas/dnas/scripts/parse_mrts.py --update --remove --enabled --ymd "${year}${month}${day}"
    done
  done
done
```

Generate stats in the DB for a specific day:
```shell
docker-compose run --rm --name tmp_stats --entrypoint /opt/pypy dnas_stats -- \
/opt/dnas/dnas/scripts/stats.py --update --enabled --daily --ymd "20230101"
```

Generate and push a report to git for a specific day:
```shell
docker-compose run --rm --name tmp_report --entrypoint /opt/pypy dnas_stats -- \
/opt/dnas/dnas/scripts/git_reports.py --generate --publish --ymd "20230101"
```

Tweet for a specific day:
```
docker-compose run --rm --name tmp_tweet --entrypoint /opt/pypy dnas_stats -- \
/opt/dnas/dnas/scripts/tweet.py --generate --tweet --ymd "20230101"
```

The script `/opt/dnas/docker/cron_script.sh` can be scheduled as a cron job to run DNAS in a retrospective mode where it generates stats for the previous day in a single run, on a daily basis, instead of continuous mode were it builds up the stats throughout the day. Note that the Redis container must be already running.

One can use the script `manual_day.sh` to run the containers in retrospective mode for a specific day: `/opt/dnas/docker/manual_day.sh 20220228`.

To run the contains in retrospective mode for yesterday one can use `/opt/dnas/docker/manual_day.sh $(date --date="1 day ago" +"%Y%m%d")`
&nbsp;
