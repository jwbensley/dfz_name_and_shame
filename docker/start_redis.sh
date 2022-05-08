#!/bin/bash

docker run -dt \
-p 6379:6379 \
-v /etc/localtime:/etc/localtime  \
-v /opt/dnas/redis/data:/data \
-v /opt/dnas/redis/redis.conf:/usr/local/etc/redis/redis.conf
--restart always \
--name dnas_redis \
redis:6.2.6 \
redis-server /usr/local/etc/redis/redis.conf

