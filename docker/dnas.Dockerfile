ARG ARCH="arm64"
ARG OS="linux"
FROM ubuntu:22.04
LABEL description="DNAS"

# Install all required programs, PyPy and modules in a single command to reduce docker image layers:
RUN apt-get update && \
DEBIAN_FRONTEND=noninteractive apt-get -y --no-install-recommends install ca-certificates wget bzip2 gzip unzip git less whois netbase vim ssh && \
apt-get clean && \
rm -rf /var/lib/apt/lists/* && \
cd /opt && \
wget https://downloads.python.org/pypy/pypy3.8-v7.3.7-aarch64.tar.bz2 && \
tar -xvf pypy3.8-v7.3.7-aarch64.tar.bz2 && \
rm pypy3.8-v7.3.7-aarch64.tar.bz2 && \
cd pypy3.8-v7.3.7-aarch64/bin/ && \
./pypy3 -m ensurepip && \
./pypy3 -mpip install --no-cache-dir --upgrade pip && \
./pypy3 -mpip install --no-cache-dir mrtparse && \
./pypy3 -mpip install --no-cache-dir requests && \
./pypy3 -mpip install --no-cache-dir redis && \
./pypy3 -mpip install --no-cache-dir tweepy

# Copy in the code directory only
COPY ./dnas/ /opt/dnas/dnas/

# Grab the SSH identify from GitHub so that the DNAS container can push commits, and premptively create SSH keys used for pushing
RUN mkdir -p /root/.ssh && \
chmod 0700 /root/.ssh && \
ssh-keyscan github.com > /root/.ssh/known_hosts && \
touch /root/.ssh/id_ed25519 && \
touch /root/.ssh/id_ed25519.pub && \
chmod 600 /root/.ssh/id_ed25519 && \
chmod 600 /root/.ssh/id_ed25519.pub

# Copy in the config file to auth as the DNAS bot
COPY ./git/gitconfig /root/.gitconfig
