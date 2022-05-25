ARG ARCH="arm64"
ARG OS="linux"
FROM ubuntu:22.04
LABEL description="DNAS"

# Keep this as one giant run command to reduce the number of layers in the image.
# Remote apt cache, pip cache, pypy tar ball etc. to also reduce the image size.
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


# This is needed to clone the reports repo as the dnasbot user:
COPY ./secrets/id_ed25519* /root/.ssh/
RUN mkdir -p /root/.ssh && \
chmod 0700 /root/.ssh && \
ssh-keyscan github.com > /root/.ssh/known_hosts && \
chmod 600 /root/.ssh/id_ed25519 && \
chmod 600 /root/.ssh/id_ed25519.pub
#touch /root/.ssh/id_ed25519 && \
#touch /root/.ssh/id_ed25519.pub && \
#chmod 600 /root/.ssh/id_ed25519 && \
#chmod 600 /root/.ssh/id_ed25519.pub
COPY ./git/gitconfig /root/.gitconfig

# Make sure this is last because the mostly likely part of the image to change is the Git repo
COPY ./dnas /opt/dnas/
COPY ./secrets/redis_auth.py /opt/dnas/dnas/
COPY ./secrets/twitter_auth.py /opt/dnas/dnas/

