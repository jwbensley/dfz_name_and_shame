# Auth Secrets

### Overview

This folder contains files/information not synced with git, but required for DNAS to run:

- ed25519 & ed25519.pub: SSH keys for DNAS bot Github account - read only deploy key added to this source code repo to pull the latest source, to build the docker container
- redis_auth.py: Auth details to connect to Redis DB container
- twitter_auth.py: Twitter API auth details fror bgp_shamer account
