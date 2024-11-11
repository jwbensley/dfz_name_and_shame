# DFZ Name and Shame Bot

## Overview

This repo contains the "code"[^1] and configuration files for the BGP DFZ Name and Shame ("DNAS") bot on Twitter: https://twitter.com/bgp_shamer

The ultimate output of this code are the daily reports published here: https://github.com/DFZ-Name-and-Shame/dnas_stats

The bot is written in Python3 and runs in several Docker containers. These containers form a rudimentary pipeline:  

* data collection (1 container continuously downloads MRT files from public and private sources as they become available)
* data ingest (1 container continuously parses the MRT as they are downloaded and the stats for each MRT file are stored in a Redis DB, consuming a real-time firehose API or similar is not currently planned)
* daily report (once per day a container is started to compile together the stats generated from all the MRTs parsed for the previous day, generate a report which is committed and pushed to GitHub, and Tweet the availability of the report and the headlines of the report)

The each stage can also be run individually in a retrospective mode, in which stats are generated and published for the previous day.

[^1]: If your standards are low you're in for a treat!

## Installation

There is a single DNAS container that is built and used for all stages of the pipeline. In addition a Redis container is required.

```bash
sudo apt-get update

# Install Docker & Docker Compose (from: https://docs.docker.com/engine/install/ubuntu/):
sudo apt-get install -y curl

curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
| sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

echo "deb [arch=$(dpkg --print-architecture) \
signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] \
https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
| sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install --no-install-recommends -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo systemctl enable docker.service
sudo systemctl enable containerd.service
sudo groupadd docker
sudo usermod -aG docker $USER
# ^ Log out and in again for this to take effect

# Set up base directory:
BASE_DIR="/opt/dnas"
sudo mkdir -p "$BASE_DIR" && sudo chown $USER:$USER "$BASE_DIR"

# Install git:
sudo apt-get install --no-install-recommends -y git

# Clone repo to base directory:
git clone git@github.com:jwbensley/dfz_name_and_shame.git "$DATA_DIR"

# Run the build script
${BASE_DIR}/docker/build_dnas.sh
```

After the steps above, DNAS is ready to run inside the containers. See documentation under [docker/](docker/) for more details.

To run DNAS "natively", not in a container, see documentation under [dnas/](dnas/).  

## Credits

Thanks to the following people/organisations/groups for their help:

* The authors of [mrtparse](https://github.com/t2mune/mrtparse) - I'm too lazy to write my own MRT parser for a spare time project, so this wouldn't be possible without them
* The [RIPE RIS](https://ris.ripe.net/docs/) project for providing public MRT file archives which allow for "research" projects like this to take place.
* The [RouteViews](http://routeviews.org/) project for providing public MRT file archives which allow for "research" projects like this to take place.
* [Mythic Beasts](https://www.mythic-beasts.com/) for the server hosting - I was hitting the Linux OOM killer on a weekly basis before they stepped in
* [Łukasz Bromirski](https://twitter.com/LukaszBromirski) - for a [full table feed](https://lukasz.bromirski.net/post/bgp-w-labie-3/)
