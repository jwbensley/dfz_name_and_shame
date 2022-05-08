#!/bin/bash

docker run -dt -p 6379:6379 --name dnas_redis --restart always -v /etc/localtime:/etc/localtime -v /opt/dnas/redis/data:/data -v /opt/dnas/redis/redis.conf:/usr/local/etc/redis/redis.conf redis:6.2.6 redis-server /usr/local/etc/redis/redis.conf

