# DNAS Docker Details

## Building DNAS

To manually the DNAS container run the build_dnas.sh script: `/opt/dnas/docker/build_dnas.sh`

## Running Containers

To run the various DNAS pipeline stages manually use the following commands.

### Redis

Run as a container in the background

`docker run -dt --restart always -p 6379:6379 -v /opt/dnas/redis/data:/data -v /opt/dnas/redis/redis.conf:/usr/local/etc/redis/redis.conf --name redis_dnas redis:6.2.6 redis-server /usr/local/etc/redis/redis.conf`

Run in interactive mode:

`docker run -it --rm -p 6379:6379 -v /opt/dnas/redis/data:/data -v /opt/dnas/redis/redis.conf:/usr/local/etc/redis/redis.conf --name redis_dnas redis:6.2.6 redis-server /usr/local/etc/redis/redis.conf`

Redis uses authentication so you won't be able to access the redis CLI without authenticating first. With the container as a daemon (using the first command above), start an interactive redis-cli process on the same container (`docker exec -it redis_dnas redis-cli`) then auth using `AUTH xxxxx` (password from redis.confg file) and test with command `PING`.

### Continuous MRT Getter

Run as a container in the background:

`docker run -td --restart always --volume /media/usb0/:/media/usb0/ -v /opt/dnas/secrets/redis_auth.py:/opt/dnas/dnas/dnas/redis_auth.py --name dnas_getter dnas:latest /opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/dnas/scripts/get_mrts.py --continuous --update`

Run in interactive mode with debugging:

`docker run -it --rm --volume /media/usb0/:/media/usb0/ -v /opt/dnas/secrets/redis_auth.py:/opt/dnas/dnas/dnas/redis_auth.py --name dnas_getter dnas:latest /opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/dnas/scripts/get_mrts.py --continuous --update --debug`


### Continuous MRT Parser

Run as a container in the background:

`docker run -dt --restart always --volume /media/usb0/:/media/usb0/ -v /opt/dnas/secrets/redis_auth.py:/opt/dnas/dnas/dnas/redis_auth.py --name dnas_parser dnas:latest /opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/dnas/scripts/parse_mrts.py --update --continuous --remove`

Run in interactive mode with debugging:

`docker run -it --rm --volume /media/usb0/:/media/usb0/ -v /opt/dnas/secrets/redis_auth.py:/opt/dnas/dnas/dnas/redis_auth.py --name dnas_parser dnas:latest /opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/dnas/scripts/parse_mrts.py --update --continuous --debug`
