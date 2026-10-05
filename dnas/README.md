# DNAS Code Details

## Running and Testing DNAS

The best way to run DNAS is inside the provided Docker containers, see [Docker](../docker/README.md).

Setup a venv for your IDE:

```shell
sudo apt-get install --no-install-recommends -y virtualenv

DATA_DIR="/opt/dnas_data/"
APP_DIR="/opt/dnas/"
VENV_DIR="./venv/"

cd "$APP_DIR"

if [ ! -d "$VENV_DIR" ]
then
    python3 -m virtualenv venv
fi
# Shellcheck can't follow "source" imports:
# shellcheck disable=SC1091
source venv/bin/activate
```

### Redis

To run the code natively (not inside a container) a Redis instance is required. The best way is to spin up the existing redis container:

```shell
cd /opt/dnas
source venv/bin/activate
cd docker/
docker compose up -d dnas_redis
```

If you really need/want to, you can use the steps below to spin up a stand-alone Redis container. Note that DNAS expects to authenticate to Redis so set a password, and update the Redis hostname in `redis_auth.py`:

```shell
docker run -d -p 6379:6379 --name redis redis:latest
docker exec -it redis_dnas redis-cli
> CONFIG SET requirepass abc123
> CONFIG REWRITE
```

### Python

The DNAS container uses the Python version set by `PYTHON_VERSION` in [docker/.env](../docker/.env). To run the DNAS code natively outside of the container, use the same Python version and install the required Python modules:

```shell
cd /opt/dnas/
source venv/bin/activate
cd ./dnas/
python3 -m pip install -r requirements.txt
```

Copy across the required authentication files:

```shell
cp "${APP_DIR}/secrets/redis_auth.py" "${APP_DIR}/dnas/dnas/"
```

### Testing

Tox is used to provide linting (black and isort), type checking (mypy), and run unit tests (pyetst):

```shell
cd /opt/dnas/
source venv/bin/activate
python3 -m pip install tox
tox

# tox -e lint
# tox -e fixlint
# tox -e mypy
# tox -e shellcheck
```

### Coding Style

Merge requests need to maintain the existing coding style[^1] for consistency:

* Stringageddon: Currently all values from MRT files are strings so that there is a single uniform data type (AS Numbers are strings, IP prefixes are strings, AS paths are list of strings etc.).
* All Redis keys are also strings.

[^1]:[In the loosest sense](https://en.wikipedia.org/wiki/Infinite_monkey_theorem)
