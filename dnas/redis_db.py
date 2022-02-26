import datetime
import json
import redis

from dnas.redis_auth import redis_auth
from dnas.mrt_stats import mrt_stats
from dnas.twitter_msg import twitter_msg

class redis_db():
    """
    Class to manage connection to Redis DB and martial data in and out.
    """

    def __init__(self):
        self.r = redis.Redis(
            host=redis_auth.host,
            port=redis_auth.port,
            password=redis_auth.password,
        )

    def add_to_queue(self, key, json_str):
        """
        Push to a list a strings.
        For example, a Tweet serialised to a JSON string.
        """
        if not key:
            raise ValueError(
                f"Missing required arguments: key={key}"
            )

        if not json_str:
            raise ValueError(
                f"Missing required arguments: json_str={json_str}"
            )

        if type(json_str) != str:
            raise TypeError(
                f"json_str is not a string: {type(json_str)}"
            )

        self.r.lpush(key, json_str)

    def close(self):
        """
        Close the redis connection.
        """
        self.r.close()

    def del_from_queue(self, key, elem):
        """
        Delete an entry from a list of strings.
        """
        if not key:
            raise ValueError(
                f"Missing required arguments: key={key}"
            )

        if not elem:
            raise ValueError(
                f"Missing required arguments: elem={elem}"
            )

        if type(elem) != str:
            raise TypeError(
                f"elem is not a string: {type(elem)}"
            )

        self.r.lrem(key, 0, elem)

    def delete(self, key):
        """
        Delete key entry in Redis.
        """
        if not key:
            raise ValueError(
                f"Missing required arguments: key={key}"
            )
        self.r.delete(key)

    def from_file(self, filename):
        """
        Restore redis DB from JSON file.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )
        with open(filename, "r") as f:
            self.from_json(f.read())

    def from_json(self, json_str):
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

    def get(self, key):
        """
        Return the value stored in "key" from Redis
        """
        if not key:
            raise ValueError(
                f"Missing required arguments: key={key}"
            )

        t = self.r.type(key).decode("utf-8")
        if t == "string":
            return self.r.get(key).decode("utf-8")
        elif t == "list":
            return [x.decode("utf-8") for x in self.r.lrange(key, 0, -1)]
        else:
            raise TypeError(
                f"Unknown redis data type stored under {key}: {t}"
            )

    def get_keys(self, pattern):
        """
        Return list of Redis keys that match search pattern.
        """
        if not pattern:
            raise ValueError(
                f"Missing required arguments: pattern={pattern}"
            )

        return [x.decode("utf-8") for x in self.r.keys(pattern)]

    def get_queue_msgs(self, key):
        """
        Return the list of Tweets stored under key as Twitter messages objects.
        """
        if not key:
            raise ValueError(
                f"Missing required arguments: key={key}"
            )

        db_q = [x for x in self.r.lrange(key, 0, -1)]
        msgs = []

        for msg in db_q:
            if msg:
                t_m = twitter_msg()
                t_m.from_json(msg.decode("utf-8"))
                msgs.append(t_m)

        return msgs

    def get_stats(self, key):
        """
        Return MRT stats from Redis as JSON, and return as an MRT stats object.
        """
        if not key:
            raise ValueError(
                f"Missing required arguments: key={key}"
            )

        mrt_s = mrt_stats()
        json_str = self.r.get(key)
        if not json_str:
            return None
        else:
            mrt_s.from_json(json_str.decode("utf-8"))
            return mrt_s

    def get_stats_json(self, key):
        """
        Return MRT stats from Redis as JSON string.
        """
        if not key:
            raise ValueError(
                f"Missing required arguments: key={key}"
            )

        return self.r.get(key).decode("utf-8")

    def set_stats(self, key, mrt_s):
        """
        Take an MRT stats object, serialise it to JSON, store in Redis.
        """
        if not key:
            raise ValueError(
                f"Missing required arguments: key={key}"
            )

        if not mrt_s:
            raise ValueError(
                f"Missing required arguments: mrt_s={mrt_s}"
            )

        self.r.set(key, mrt_s.to_json())

    def set_stats_json(self, key, json_str):
        """
        Take JSON serialisation of an MRT stats object, and store in Redis.
        """
        if not key:
            raise ValueError(
                f"Missing required arguments: key={key}"
            )

        if not json_str:
            raise ValueError(
                f"Missing required arguments: json_str={json_str}"
            )

        self.r.set(key, json_str)

    def to_file(self, filename):
        """
        Dump the entire redis DB to a JSON file.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        with open(filename, "w") as f:
            f.write(self.to_json())

    def to_json(self):
        """
        Dump the entire redis DB to JSON
        """
        d = {}
        for k in self.r.keys("*"):
            k = k.decode("utf-8")
            d[k] = self.r.get(k).decode("utf-8")
        return json.dumps(d)
