# DNAS Docker Details

## Automated Build and Run

### In Pipeline Mode - With Docker Compose

To build the Redis DB container and DNAS container use the following:

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
```

To shut the entire pipeline down simply run: `docker-compose down`

On Ubuntu the `ufw` may block connections between containers. This will allow REDIS access: `sudo ufw allow from 172.16.0.0/12 to 172.16.0.0/12 proto tcp port 6379` (TODO: this should be locked down further)
This will allow access to BIRD: `sudo ufw allow from 172.16.0.0/12 to 172.16.0.0/12 proto tcp port 8000`

To start an individual container from the pipeline use: `docker-compose up -d dnas_redis`

To stop and remove an individual container usei: `docker-compose stop dnas_redis && docker-compose rm dnas_redis`

To run BASH on a container use a custom entry point: `docker-compose run --entrypoint /bin/bash --rm dnas_redis`  
&nbsp;

### In Pipeline Mode - Without Docker Compose

Use the commands below to run the pipeline containers individually.

#### Redis

Run as a container in the background

```bash
docker run -dt \
-p 6379:6379 \
-v /etc/localtime:/etc/localtime \
-v /opt/dnas/redis/data:/data \
-v /opt/dnas/redis/redis.conf:/usr/local/etc/redis/redis.conf \
--restart always \
--name dnas_redis \
redis:6.2.6 \
redis-server /usr/local/etc/redis/redis.conf
```

Run in interactive mode:

```bash
docker run -it --rm \
-p 6379:6379 \
-v /etc/localtime:/etc/localtime \
-v /opt/dnas/redis/data:/data \
-v /opt/dnas/redis/redis.conf:/usr/local/etc/redis/redis.conf \
--name dnas_redis \
redis:6.2.6 \
redis-server /usr/local/etc/redis/redis.conf
```

Redis uses authentication so you won't be able to access the redis CLI without authenticating first. With the container as a daemon (using the first command above), start an interactive redis-cli process on the same container (`docker exec -it dnas_redis redis-cli`). Use the auth using in the Redis CLI `AUTH xxxxx` (password from redis.conf file) and test with command `PING`.  
&nbsp;

#### Continuous MRT Getter

Note: the local time configuration file from the host is shared into the container because the host is not in UTC0/GMT time, but most MRT dumps use UTC0 timestamps.

Run as a container in the background:

```bash
docker run -td \
-v /etc/localtime:/etc/localtime \
-v /opt/dnas_data/;/opt/dnas_data/ \
--restart always \
--name dnas_getter \
dnas:latest \
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/scripts/get_mrts.py --continuous --update
```

Run in interactive mode with debugging:

```bash
docker run -it --rm \
-v /etc/localtime:/etc/localtime \
-v /opt/dnas_data/:/opt/dnas_data/ \
--name dnas_getter \
dnas:latest \
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/scripts/get_mrts.py --continuous --update --debug
```
&nbsp;

#### Continuous MRT Parser

Run as a container in the background:

```bash
docker run -dt \
-v /etc/localtime:/etc/localtime \
-v /opt/dnas_data/:/opt/dnas_data/ \
--restart always \
--name dnas_parser \
dnas:latest \
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/scripts/parse_mrts.py --update --continuous --remove
```

Run in interactive mode with debugging:

```bash
docker run -it --rm \
-v /etc/localtime:/etc/localtime \
-v /opt/dnas_data/:/opt/dnas_data/ \
--restart always \
--name dnas_parser \
dnas:latest \
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/scripts/parse_mrts.py --update --continuous --remove --debug
```
&nbsp;

### In Retrospective Mode

The script `/opt/dnas/docker/cron_script.sh` can be scheduled as a cron job to run DNAS in a retrospective mode where it generates states for the previous day in a single run, on a daily basis, instead of continuous mode were it builds up the stats throughout the day. Note that the Redis container must be already running.

## Manual Build and Run

One can run `docker-compose build` to rebuild the Redis and DNAS containers any time. However this doesn't pull the latest software version from Git. To pull the latest code from the Git repo and rebuild the DNAS container run the build script: `/opt/dnas/docker/build_dnas.sh`

One can use the script `day.sh` to run the containers in retrospective mode for a specific day: `/opt/dnas/docker/day.sh 20220228` or `/opt/dnas/docker/day.sh $(date --date="1 day ago" +"%Y%m%d")`  
&nbsp;

One can use the script `yesterday.sh` to run the containers in retrospective mode for yesterday: `/opt/dnas/docker/yesterday.sh`  
&nbsp;

Note that when manually running containers, they join the docker network "docker0", whereas when running containers user docker-compose they join the network "docker_default". Containers manually started using `docker run ...` which join "docker0" might not be able to communicate with containers started using `docker-compuse -d up ...`. This seems to be an UFW issue on Ubuntu. Also when starting containers manuall with `docker run ...` which expose ports, the folling is needed to allow other containers in "docker0" to access those exposed ports: `sudo ufw allow in on docker0 proto tcp from 172.16.0.0/12`.
