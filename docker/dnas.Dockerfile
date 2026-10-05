# ARG before FROM is only in scope for FROM, it must be redeclared after FROM to be used later
ARG PYTHON_VERSION=3.14
FROM python:${PYTHON_VERSION}-slim-trixie
LABEL description="dnas"

# Keep this as one giant run command to reduce the number of layers in the image.
# Remove apt cache, pip cache etc. to also reduce the image size.
RUN apt-get update
RUN apt-get -y --no-install-recommends install \
ca-certificates wget bzip2 gzip unzip git less whois netbase vim ssh cron && \
apt-get clean && \
rm -rf /var/lib/apt/lists/* && \
/usr/local/bin/python3 -V

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
RUN /usr/local/bin/python3 -m pip install --no-cache-dir --root-user-action=ignore --upgrade pip && \
/usr/local/bin/python3 -m pip install --no-cache-dir --root-user-action=ignore -r /opt/dnas/requirements.txt

# Then copy the rest of the files because these often change, to avoid having to pip install each time
COPY ./dnas/ /opt/dnas/

# Set up the crontab function for the daily stats container
COPY ./docker/cron_script.sh /opt/dnas/docker/cron_script.sh
COPY ./docker/cronfile /opt/dnas/docker/cronfile
RUN chmod 0755 /opt/dnas/docker/cron_script.sh && \
chmod 0644 /opt/dnas/docker/cronfile && \
crontab /opt/dnas/docker/cronfile

# "Whatever is specified in the command in docker-compose.yml should get
# appended to the entrypoint defined in the Dockerfile, provided entrypoint
# is defined in exec form in the Dockerfile:
ENTRYPOINT ["/usr/local/bin/python3"]
