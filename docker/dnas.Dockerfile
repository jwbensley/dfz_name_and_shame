ARG ARCH="x64"
ARG OS="linux"
FROM ubuntu:22.04
LABEL description="DNAS"

# Keep this as one giant run command to reduce the number of layers in the image.
# Remote apt cache, pip cache, pypy tar ball etc. to also reduce the image size.
RUN apt-get update -qq && \
DEBIAN_FRONTEND=noninteractive apt-get -y --no-install-recommends -qq install \
ca-certificates wget bzip2 gzip unzip git less whois netbase vim ssh cron && \
apt-get clean && \
rm -rf /var/lib/apt/lists/* && \
cd /opt && \
wget https://downloads.python.org/pypy/pypy3.8-v7.3.7-linux64.tar.bz2 && \
tar -xf pypy3.8-v7.3.7-linux64.tar.bz2 && \
rm pypy3.8-v7.3.7-linux64.tar.bz2

# This is needed to clone the reports repo as the dnasbot user:
RUN mkdir -p /root/.ssh && \
chmod 0700 /root/.ssh && \
ssh-keyscan github.com > /root/.ssh/known_hosts
COPY ./secrets/id_ed25519* /root/.ssh/
RUN chmod 600 /root/.ssh/id_ed25519 && \
chmod 600 /root/.ssh/id_ed25519.pub
COPY ./git/gitconfig /root/.gitconfig
COPY ./secrets/redis_auth.py /opt/dnas/dnas/
COPY ./secrets/twitter_auth.py /opt/dnas/dnas/

# Copy just the requirements file because this rarely changes
COPY ./dnas/requirements.txt /opt/dnas/requirements.txt
# Then install the requirements
RUN /opt/pypy3.8-v7.3.7-linux64/bin/pypy3 -m ensurepip && \
/opt/pypy3.8-v7.3.7-linux64/bin/pypy3 -mpip install --no-cache-dir --upgrade pip && \
/opt/pypy3.8-v7.3.7-linux64/bin/pypy3 -mpip install -r /opt/dnas/requirements.txt

# Then copy the rest of the files because these often change, to avoid having to pip install each time
COPY ./dnas/ /opt/dnas/

# Set up the crontab function for the daily stats container
COPY ./docker/cron_script.sh /opt/dnas/docker/cron_script.sh
COPY ./docker/cronfile /opt/dnas/docker/cronfile
RUN chmod 0755 /opt/dnas/docker/cron_script.sh && \
chmod 0644 /opt/dnas/docker/cronfile && \
crontab /opt/dnas/docker/cronfile

