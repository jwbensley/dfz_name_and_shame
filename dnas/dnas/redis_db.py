import json
from typing import Union

from dnas.mrt_stats import mrt_stats
from dnas.redis_auth import redis_auth  # type: ignore
from dnas.twitter_msg import twitter_msg
from redis.client import Redis


class redis_db:
    """
    Class to manage connection to Redis DB and martial data in and out.
    """

    def __init__(self: "redis_db") -> None:
        self.r = Redis(
            host=redis_auth.host,
            port=redis_auth.port,
            password=redis_auth.password,
        )

    def add_to_queue(self: "redis_db", key: str, json_str: str) -> None:
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

        self.r.lpush(key, json_str)

    def close(self: "redis_db") -> None:
        """
        Close the redis connection.
        """
        self.r.close()

    def del_from_queue(self: "redis_db", key: str, elem: str) -> None:
        """
        Delete an entry from a list of strings.
        """
        if not key or not elem:
            raise ValueError(
                f"Missing required arguments: key={key}, elem={elem}"
            )

        if type(elem) != str:
            raise TypeError(f"elem is not a string: {type(elem)}")

        self.r.lrem(key, 0, elem)

    def delete(self: "redis_db", key: str) -> int:
        """
        Delete key entry in Redis.
        """
        if not key:
            raise ValueError(f"Missing required arguments: key={key}")

        return self.r.delete(key)

    def from_file(self: "redis_db", filename: str) -> None:
        """
        Restore redis DB from JSON file.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        with open(filename, "r") as f:
            self.from_json(f.read())

    def from_json(self: "redis_db", json_str: str):
        """
        Restore redis DB from a JSON string
        """
        if not json_str:
            raise ValueError(
                f"Missing required arguments: json_str={json_str}"
            )

        json_dict = json.loads(json_str)
        for k in json_dict.keys():
            self.r.set(k, json_dict[k])

    def get(self: "redis_db", key: str) -> Union[str, list]:
        """
        Return the value stored in "key" from Redis
        """
        if not key:
            raise ValueError(f"Missing required arguments: key={key}")

        t = self.r.type(key).decode("utf-8")
        if t == "string":
            val = self.r.get(key)
            if val:
                return val.decode("utf-8")
            else:
                raise ValueError(
                    f"Couldn't decode data stored under key {key}"
                )
        elif t == "list":
            return [x.decode("utf-8") for x in self.r.lrange(key, 0, -1)]
        else:
            raise TypeError(f"Unknown redis data type stored under {key}: {t}")

    def get_keys(self: "redis_db", pattern: str) -> list:
        """
        Return list of Redis keys that match search pattern.
        """
        if not pattern:
            raise ValueError(f"Missing required arguments: pattern={pattern}")

        return [x.decode("utf-8") for x in self.r.keys(pattern)]

    def get_queue_msgs(self: "redis_db", key: str) -> list["twitter_msg"]:
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
                t_m.from_json(msg.decode("utf-8"))
                msgs.append(t_m)

        return msgs

    def get_stats(self: "redis_db", key: str) -> Union[None, "mrt_stats"]:
        """
        Return MRT stats from Redis as JSON, and return as an MRT stats object.
        """
        if not key:
            raise ValueError(f"Missing required arguments: key={key}")

        mrt_s = mrt_stats()
        json_str = self.r.get(key)
        if not json_str:
            return None
        else:
            mrt_s.from_json(json_str.decode("utf-8"))
            return mrt_s

    def set_stats(self: "redis_db", key: str, mrt_s: "mrt_stats"):
        """
        Take an MRT stats object, serialise it to JSON, store in Redis.
        """
        if not key or not mrt_s:
            raise ValueError(
                f"Missing required arguments: key={key}, mrt_s={mrt_s}"
            )

        self.r.set(key, mrt_s.to_json())

    def set_stats_json(self: "redis_db", key: str, json_str: str):
        """
        Take JSON serialisation of an MRT stats object, and store in Redis.
        """
        if not key:
            raise ValueError(f"Missing required arguments: key={key}")

        if not json_str:
            raise ValueError(
                f"Missing required arguments: json_str={json_str}"
            )

        self.r.set(key, json_str)

    def to_file(self: "redis_db", filename: str):
        """
        Dump the entire redis DB to a JSON file.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        with open(filename, "w") as f:
            f.write(self.to_json())

    def to_json(self: "redis_db") -> str:
        """
        Dump the entire redis DB to JSON
        """
        d: dict = {}
        for k in self.r.keys("*"):
            t = self.r.type(k).decode("utf-8")
            if t == "string":
                val = self.r.get(k)
                if val:
                    d[k.decode("utf-8")] = val.decode("utf-8")
                else:
                    raise ValueError(
                        f"Couldn't decode data stored under key {k.decode('utf-8')}"
                    )
            elif t == "list":
                d[k.decode("utf-8")] = [
                    x.decode("utf-8") for x in self.r.lrange(k, 0, -1)
                ]
            else:
                raise TypeError(
                    f"Unsupported data type {t} stored under key {k.decode('utf-8')}"
                )

        if d:
            return json.dumps(d)
        else:
            raise ValueError("Database is empty")
