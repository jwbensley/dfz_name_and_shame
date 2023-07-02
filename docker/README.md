# DNAS Docker Details

## Automated Build and Run

### In Pipeline Mode

To build the Redis DB container and DNAS containers use the following:

```bash
cd /opt/dnas/
source venv/bin/activate
cd docker/
docker-compose build
```

Use docker-compose to start the Redis container and the continuous-MRT-getter and continuous-MRT-parser containers:

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

#### Continuous MRT Parser

### In Retrospective Mode

PyPy Path:
```shell
pypy="/opt/pypy3.8-v7.3.7-linux64/bin/pypy3"
```

Pull any missing MRTs for a specific day:
```shell
docker-compose run --rm --name tmp_getter --entrypoint \
"${pypy}" \
dnas_getter -- \
/opt/dnas/scripts/get_mrts.py --backfill --update --enabled --range --ymd "20210101"
```

Run an the parser for a specific day:
```shell
docker-compose run --rm --name tmp_parser --entrypoint \
"${pypy}" \
dnas_parser -- \
/opt/dnas/scripts/parse_mrts.py --update --remove --enabled --ymd "20210101"
```

Run the parser for a specfic file:
```shell
docker-compose run --rm --name tmp_parser --entrypoint \
"${pypy}" \
dnas_parser -- \
/opt/dnas/scripts/parse_mrts.py --debug --remove --single /opt/dnas_data/downloads/SYDNEY/updates.20230424.0615.bz2
```

Run the parser for a specific time range (this can be less than a day or longer than a day):
```shell
docker-compose run --rm --name tmp_parser --entrypoint \
"${pypy}" \
dnas_parser -- \
/opt/dnas/scripts/parse_mrts.py --update --remove --enabled --start "20210101.0000" --end "20210101.2359"
```

Generate stats in the DB for a specific day:
```shell
docker-compose run --rm --name tmp_stats --entrypoint \
"${pypy}" \
dnas_stats -- \
/opt/dnas/scripts/stats.py --update --enabled --daily --ymd "$1"
```

Generate and push a report to git for a specific day:
```shell
docker-compose run --rm --name tmp_report --entrypoint \
"${pypy}" \
dnas_stats -- \
/opt/dnas/scripts/git_reports.py --generate --publish --ymd "$1"
```

The script `/opt/dnas/docker/cron_script.sh` can be scheduled as a cron job to run DNAS in a retrospective mode where it generates stats for the previous day in a single run, on a daily basis, instead of continuous mode were it builds up the stats throughout the day. Note that the Redis container must be already running.

## Manual Build and Run

One can run `docker-compose build` to rebuild the Redis and DNAS containers any time. However this doesn't pull the latest software version from Git. To pull the latest code from the Git repo and rebuild the DNAS container run the build script: `/opt/dnas/docker/build_dnas.sh`

One can use the script `manual_day.sh` to run the containers in retrospective mode for a specific day: `/opt/dnas/docker/manual_day.sh 20220228`.

To run the contains in retrospective mode for yesterday one can use `/opt/dnas/docker/manual_day.sh $(date --date="1 day ago" +"%Y%m%d")`
&nbsp;

One can use the script `yesterday.sh` to run the containers in retrospective mode for yesterday: `/opt/dnas/docker/yesterday.sh`  
&nbsp;
