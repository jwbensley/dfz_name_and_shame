#!/bin/bash

docker run -td \
-v /etc/localtime:/etc/localtime \
-v /media/usb0/:/media/usb0/ \
--restart always \
--name dnas_getter \
dnas:latest \
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 /opt/dnas/scripts/get_mrts.py --continuous --update

