# DNAS Code Details

## Requirements


### Redis
To run the code locally a redis instance is required. Use the steps below to quickly spin up a Redis container. Note that DNAS expects to authenticate to Redis so set a password:  

```bash
$ docker run -d -p 6379:6379 --name redis redis:latest
$ docker exec -it redis_dnas redis-cli
> CONFIG SET requirepass abc123
> CONFIG REWRITE
```

### Python & PyPy

Install the required Python modules:

```
cd /opt/dnas/
source venv/bin/activate
cd ./dnas/
pip install -r requirements.txt
```

The code is developed in Python3 but the DNAS container actually uses PyPy3 to run faster. The following commands manually install PyPy and the required modules in PyPy:

```bash
cd /tmp
wget https://downloads.python.org/pypy/pypy3.8-v7.3.7-aarch64.tar.bz2
tar -xvf pypy3.8-v7.3.7-aarch64.tar.bz2
rm pypy3.8-v7.3.7-aarch64.tar.bz2
sudo mv pypy3.8-v7.3.7-aarch64/ /opt/

/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 -mpip ensurepip
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 -mpip install --upgrade pip
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 -mpip install mrtparse
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 -mpip install requests
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 -mpip install redis
/opt/pypy3.8-v7.3.7-aarch64/bin/pypy3 -mpip install tweepy
````

### Testing

MyPy type checking is performed using the following:

```bash
cd /opt/dnas/
source venv/bin/activate
cd ./dnas/
pip install mypy
pip install types-redis
pip install types-requests
mypy dnas/*.py
mypy scripts/*.py
```


### Download MRT files

```bash
$./scripts/get_mrts.py --range --start 20220103.0000 --end 20220103.2359 --debug --update
```

### Parse MRT files

```bash

```


### Coding Style

Merge requests need to maintain the existing coding style[^1] for consistency:

* Stringageddon: Currently all values from MRT files are strings so that there is a single uniform data type (AS Numbers are strings, IP prefixes are strings, AS paths are list of strings etc.).
* All Redis keys are also strings.


[^1]:[In the loosest sense](https://en.wikipedia.org/wiki/Infinite_monkey_theorem)
