#!/bin/bash

docker run -td --restart always -v /etc/localtime:/etc/localtime -v /media/usb0/:/media/usb0/ -v /opt/dnas/secrets/redis_auth.py:/opt/dnas/dnas/dnas/redis_auth.py --name dnas_getter dnas /opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/dnas/scripts/get_mrts.py --continuous --update

