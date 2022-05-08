#!/bin/bash

docker run -dt -v /etc/localtime:/etc/localtime -v /media/usb0/:/media/usb0/ -v /opt/dnas/secrets/redis_auth.py:/opt/dnas/dnas/dnas/redis_auth.py --restart always --name dnas_parser dnas /opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/dnas/scripts/parse_mrts.py --update --continuous --remove

