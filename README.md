# DFZ Name and Shame Bot

## Overview

This repo contains the "code"[^1] and configuration files for the BGP DFZ Name and Shame ("DNAS") bot on Twitter: https://twitter.com/bgp_shamer

The bot is written in Python3 and runs in several Docker containers. These containers form a rudimentary pipeline:  

* data collection (1 container continuously downloads MRT files from public and private sources as they become available)
* data ingest (1 container continuously parses the MRT as they are downloaded and the stats for each MRT file are stored in a Redis DB, consuming a real-time firehose API or similar is not currently planned)
* daily report (once per day a container is started to compile together the stats generated from all the MRTs parsed for the previous day, generate a report which is committed and pushed to GitHub, and Tweet the availability of the report and the headlines of the report)

The each stage can also be run individually in a retrospective mode, in which stats are generated and published for the previous day.

[^1]: If your standards are low you're in for a treat!

## Installation

There is a single DNAS container that is built and used for all stages of the pipeline. In addition a Redis container is required.

* Install git: `sudo apt-get install -y git`
* Install Docker: https://docs.docker.com/engine/install/
* Install virtualenv: `sudo apt-get install -y virtualenv`
* Set up base directory: `sudo mkdir /opt/dnas/ && sudo chown $USER:$USER /opt/dnas/`
* Install docker-compose `cd /opt/dnas/ && virtualenv venv && source venv/bin/activate && pip3 install docker-compose`
* Clone this repo: `git clone git@github.com:jwbensley/dfz_name_and_shame.git /opt/dnas`

## Running

See details in [docker/](docker/)

## Credits

Thanks to the following people for their help:

* Łukasz Bromirski - for a full table feed

