import base64
import gzip
import json
import logging
from typing import Any, Callable, Iterable, Union

from dnas.mrt_stats import mrt_stats
from dnas.redis_auth import redis_auth  # type: ignore
from dnas.twitter_msg import twitter_msg
from redis.exceptions import ConnectionError

from redis import Redis


class redis_db:
    """
    Class to manage connection to Redis DB and martial data in and out.
    """

    class RedisConnectFailure(Exception):
        pass

    class RedisDecompressionFailure(Exception):
        pass

    class RedisGetFailure(Exception):
        pass

    def __init__(self: "redis_db") -> None:
        self.r: Redis = Redis(
            host=redis_auth.host,
            port=redis_auth.port,
            password=redis_auth.password,
        )
        # Check we have connected:
        self.ping()
        logging.debug("Connected to Redis DB")

    def add_to_queue(
        self: "redis_db", key: str, json_str: str, compression: bool = True
    ) -> None:
        """
        Push to a list a strings.
        For example, a Tweet serialised to a JSON string.
        """
        if not key or not json_str:
            raise ValueError(
                f"Missing required arguments: key={key}, json_str={json_str}"
            )

        if type(json_str) != str:
            raise TypeError(f"json_str is not a string: {type(json_str)}")

        logging.debug(
            f"Pushing string to list {key} (compression={compression}):\n{json_str}"
        )

        if compression:
            self.r.lpush(key, redis_db.compress(json_str))
        else:
            self.r.lpush(key, json_str)

    def close(self: "redis_db") -> None:
        """
        Close the redis connection.
        """
        self.r.close()

    @staticmethod
    def compress(data: str) -> str:
        """
        Gzip compress the import data (result in compressed binary data)
        Return a base85 encoded string of the compressed binary data.
        """
        compressed = gzip.compress(
            data=bytes(data, encoding="utf-8"), compresslevel=9
        )
        b85 = base64.b85encode(compressed)
        return b85.decode("utf-8")

    @staticmethod
    def decompress(data: str) -> str:
        """
        Take in a base85 encoded string, decompress it to the original gzip
        binary data, and then decompress that to the original string
        """
        try:
            compressed = base64.b85decode(data)
        except ValueError:
            raise redis_db.RedisDecompressionFailure(
                f"Failed to decode base85 str to binary: {data}"
            )
        uncompressed = gzip.decompress(compressed)
        return uncompressed.decode("utf-8")

    def del_from_queue(
        self: "redis_db", key: str, elem: str, compression: bool = True
    ) -> None:
        """
        Delete an entry from a list of strings.
        """
        if not key or not elem:
            raise ValueError(
                f"Missing required arguments: key={key}, elem={elem}"
            )

        if type(elem) != str:
            raise TypeError(f"elem is not a string: {type(elem)}")

        if compression:
            self.r.lrem(key, 0, redis_db.compress(elem))
        else:
            self.r.lrem(key, 0, elem)

    def delete(self: "redis_db", key: str) -> int:
        """
        Delete key entry in Redis.
        """
        if not key:
            raise ValueError(f"Missing required arguments: key={key}")

        return self.r.delete(key)

    def from_file(
        self: "redis_db",
        filename: str,
        compression: bool = True,
        daily_only: bool = False,
    ) -> None:
        """
        Restore redis DB from a JSON file, or a gzip compressed JSON file.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        if filename.endswith(".gz"):
            with gzip.open(filename, "rt", encoding="utf-8") as f:
                raw = json.load(f)
        elif filename.endswith(".json"):
            with open(filename, "r") as f:
                raw = json.load(f)
        else:
            raise ValueError(
                f"Unsupported file extension, expected .gz or .json: "
                f"{filename}"
            )

        if daily_only:
            for k in list(raw.keys()):
                if not k.startswith("DAILY:"):
                    del raw[k]

        self.from_json(raw, compression=compression)

    @staticmethod
    def _iter_stream_kvs(f: Any, chunk_size: int = 1 << 20) -> Iterable:
        """
        Incrementally parse a top level JSON object of the form
        {"key": "value", "key2": ["v1", "v2"], ...} from the file-like
        object f, yielding (key, value) tuples.

        This reads the file in large chunks and uses the C accelerated
        json decoder to pull out each key/value pair, instead of scanning
        the file one character at a time in Python, which is orders of
        magnitude slower on large files.
        """
        decoder = json.JSONDecoder()
        buf = f.read(chunk_size)
        pos = 0

        def ensure_data() -> bool:
            nonlocal buf
            chunk = f.read(chunk_size)
            if not chunk:
                return False
            buf += chunk
            return True

        def skip_chars(chars: str) -> None:
            nonlocal pos
            while True:
                while pos < len(buf) and buf[pos] in chars:
                    pos += 1
                if pos < len(buf) or not ensure_data():
                    return

        def decode_next() -> Any:
            nonlocal pos
            while True:
                try:
                    obj, pos = decoder.raw_decode(buf, pos)
                    return obj
                except json.JSONDecodeError:
                    if not ensure_data():
                        raise

        skip_chars(" \t\r\n")
        if pos >= len(buf) or buf[pos] != "{":
            raise ValueError("Expected JSON stream to start with '{'")
        pos += 1

        while True:
            skip_chars(" \t\r\n,")
            if pos >= len(buf):
                raise ValueError("Unexpected end of JSON stream")
            if buf[pos] == "}":
                break

            key = decode_next()
            skip_chars(" \t\r\n:")
            value = decode_next()
            yield key, value

            # Drop the consumed prefix so the buffer doesn't grow to hold
            # the entire remainder of the file.
            buf = buf[pos:]
            pos = 0

    def from_file_stream(
        self: "redis_db",
        filename: str,
        compression: bool = True,
        daily_only: bool = False,
    ) -> None:
        """
        Restore redis DB from a JSON file, or a gzip compressed JSON file,
        loading the content one key/value pair at a time.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        if filename.endswith(".gz"):
            open_func: Callable[..., Any] = gzip.open
        elif filename.endswith(".json"):
            open_func = open
        else:
            raise ValueError(
                f"Unsupported file extension, expected .gz or .json: "
                f"{filename}"
            )

        loaded_kvs = 0
        with open_func(filename, "rt", encoding="utf-8") as f:
            for key, value in redis_db._iter_stream_kvs(f):
                if daily_only and not key.startswith("DAILY:"):
                    continue

                logging.info(f"Loading key: {key} into Redis")
                if type(value) == str:
                    self.set(key=key, value=value, compression=compression)
                elif type(value) == list:
                    for elem in value:
                        self.add_to_queue(
                            key=key,
                            json_str=json.dumps(elem),
                            compression=compression,
                        )
                else:
                    raise TypeError(
                        f"Value for key {key} decoded to type {type(value)} "
                        f"is unexpected"
                    )
                loaded_kvs += 1
                logging.debug(f"Loaded {loaded_kvs} k/v's from stream")

    def from_json(
        self: "redis_db", data: str | dict[Any, Any], compression: bool = True
    ):
        """
        Restore redis DB from a JSON string
        """
        if not data:
            raise ValueError(f"Missing required arguments: data={data}")

        if type(data) == str:
            try:
                json_dict = json.loads(data)
            except json.decoder.JSONDecodeError as e:
                raise ValueError(f"Failed to decode JSON string {e}\n{data}")
        elif type(data) == dict:
            json_dict = data
        else:
            raise TypeError(f"Expected str or dict, got {type(data)}")

        for k in json_dict.keys():
            t = type(json_dict[k])
            if t == str:
                self.set(key=k, value=json_dict[k], compression=compression)
            elif t == list:
                for elem in json_dict[k]:
                    if type(elem) != str:
                        raise TypeError(
                            f"List elem is not a string: {type(elem)}"
                        )
                    self.add_to_queue(
                        key=k,
                        json_str=elem,
                        compression=compression,
                    )
            else:
                raise TypeError(
                    f"Value for key {k} decoded to type {type(json_dict[k])} "
                    f"is unexpected"
                )

        logging.info(f"Loaded {len(json_dict)} k/v's")

    def get(
        self: "redis_db", key: str, compression: bool = True
    ) -> Union[str, list[str]]:
        """
        Return the value stored in "key" from Redis
        """
        if not key:
            raise ValueError(f"Missing required arguments: key={key}")

        t = self.r.type(key).decode("utf-8")
        if t == "string":
            val = self.r.get(key)
            if val:
                if compression:
                    try:
                        return redis_db.decompress(val.decode("utf-8"))
                    except self.RedisDecompressionFailure as e:
                        raise self.RedisGetFailure(
                            f"Failed to decompress value stored under key "
                            f"{key}: {e}"
                        )
                else:
                    return val.decode("utf-8")
            else:
                raise ValueError(
                    f"Couldn't decode data stored under key {key}"
                )
        elif t == "list":
            if compression:
                try:
                    return [
                        redis_db.decompress(x.decode("utf-8"))
                        for x in self.r.lrange(key, 0, -1)
                    ]
                except self.RedisDecompressionFailure as e:
                    raise self.RedisGetFailure(
                        f"Failed to decompress value stored under key "
                        f"{key}: {e}"
                    )
            else:
                return [x.decode("utf-8") for x in self.r.lrange(key, 0, -1)]
        elif t == "none":
            logging.debug(f"Key {key} doesn't exist in Redis")
            return ""
        else:
            raise TypeError(f"Unknown redis data type stored under {key}: {t}")

    def get_keys(self: "redis_db", pattern: str) -> list[str]:
        """
        Return list of Redis keys that match search pattern.
        """
        if not pattern:
            raise ValueError(f"Missing required arguments: pattern={pattern}")

        return [x.decode("utf-8") for x in self.r.keys(pattern)]

    def get_queue_msgs(
        self: "redis_db", key: str, compression: bool = True
    ) -> list["twitter_msg"]:
        """
        Return the list of Tweets stored under key as Twitter messages objects.
        """
        if not key:
            raise ValueError(f"Missing required arguments: key={key}")

        """
        Return from list in reverse order, to present items in the same order
        they went into the queue/list:
        """
        db_q = [x for x in self.r.lrange(key, 0, -1)]
        db_q.reverse()
        msgs = []

        for msg in db_q:
            if msg:
                t_m = twitter_msg()
                if compression:
                    try:
                        t_m.from_json(self.decompress(msg.decode("utf-8")))
                    except self.RedisDecompressionFailure as e:
                        raise self.RedisGetFailure(
                            f"Failed to decompress value stored under key "
                            f"{key}: {e}"
                        )
                else:
                    t_m.from_json(msg.decode("utf-8"))
                msgs.append(t_m)

        return msgs

    def get_stats(
        self: "redis_db", key: str, compression: bool = True
    ) -> Union[None, "mrt_stats"]:
        """
        Return MRT stats from Redis as JSON, and return as an MRT stats object.
        """
        if not key:
            raise ValueError(f"Missing required arguments: key={key}")

        json_str = self.get(key, compression=compression)
        assert type(json_str) == str
        if not json_str:
            logging.debug(f"Empty day stats key {key}")
            return None

        mrt_s = mrt_stats()
        mrt_s.from_json(json_str)
        return mrt_s

    def ping(self: "redis_db") -> None:
        try:
            assert self.r.ping()
        except ConnectionError as e:
            raise self.RedisConnectFailure(
                f"Unable to PING redis server "
                f"{redis_auth.host}:{redis_auth.port}\n{e}"
            )

    def set(self: "redis_db", key: str, value: str, compression: bool = True):
        """
        Take a key and a string and store it in Redis.
        """
        if not key or not value:
            raise ValueError(
                f"Missing required arguments: key={key}, value={value}"
            )

        # logging.debug(f"Setting key {key} to value (compression={compression}):\n{value}")
        if compression:
            self.r.set(key, redis_db.compress(value))
        else:
            self.r.set(key, value)

    def to_file(self: "redis_db", filename: str, compression: bool = True):
        """
        Dump the entire redis DB to a JSON file, or a gzip compressed JSON.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        if filename.endswith(".gz"):
            with gzip.open(filename, "wt", encoding="utf-8") as f:
                f.write(self.to_json(compression=compression))
        elif filename.endswith(".json"):
            with open(filename, "w") as f:
                f.write(self.to_json(compression=compression))
        else:
            raise ValueError(
                f"Unsupported file extension, expected .gz or .json: "
                f"{filename}"
            )

    def to_file_stream(
        self: "redis_db", filename: str, compression: bool = True
    ):
        """
        to_json returns a giant dict of the entire DB which can be serialised
        as a JSON string. The DB is now too big for this (the server runs out
        of memory). Instead, write the DB to file, one key at a time:
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        if filename.endswith(".gz"):
            open_func: Callable[..., Any] = gzip.open
        elif filename.endswith(".json"):
            open_func = open
        else:
            raise ValueError(
                f"Unsupported file extension, expected .gz or .json: "
                f"{filename}"
            )

        with open_func(filename, "wt", encoding="utf-8") as f:
            elem_count = 0
            for line in self.to_json_stream(compression=not compression):
                f.write(line)
                elem_count += 1
                logging.debug(
                    f"Wrote {elem_count} elements to file from stream"
                )
        logging.debug(
            f"Two more elements should be written than the total number of k/v "
            "pairs"
        )

    def to_json(self: "redis_db", compression: bool = True) -> str:
        """
        Dump the entire redis DB to JSON
        """
        logging.debug(f"Dumping Redis to JSON, {compression=}")
        d: dict = {}
        for k in self.r.keys("*"):
            d[k.decode("utf-8")] = self.get(key=k, compression=compression)

        if d:
            json_str = json.dumps(d)
            logging.info(f"Dumped {len(d)} k/v's")
            return json_str
        else:
            raise ValueError("Database is empty")

    def to_json_stream(self: "redis_db", compression: bool = True) -> Iterable:
        logging.debug(f"Streaming Redis to JSON, {compression=}")
        yield ("{")
        keys = self.get_keys("*")
        for idx, key in enumerate(keys):
            """
            This could be the key for a string or a list in Redis.
            """
            val = self.get(key=key, compression=compression)
            if type(val) == str:
                json_str = json.dumps({key: val})
                # Strip the curly brackets
                json_str = json_str[1:-1]
            elif type(val) == list:
                json_str = f'"{key}": {json.dumps(val)}'
            else:
                raise TypeError(
                    f"Key {key} decoded to type {type(val)} which is "
                    f"unexpected"
                )
            if idx == len(keys) - 1:
                # Strip the curly brackets
                yield (json_str)
            else:
                yield (json_str + ", ")
        yield ("}")
        logging.info(f"Dumped {len(keys)} k/v's as stream")
