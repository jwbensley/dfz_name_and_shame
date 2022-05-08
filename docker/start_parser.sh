#!/bin/bash

docker run -dt \
-v /etc/localtime:/etc/localtime \
-v /media/usb0/:/media/usb0/ \
--restart always \
--name dnas_parser \
dnas:latest \
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/scripts/parse_mrts.py --update --continuous --remove

