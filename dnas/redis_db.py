import datetime
import json
import redis

from dnas.redis_auth import redis_auth
from dnas.mrt_stats import mrt_stats

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

    def close(self):
        """
        Close the redis connection.
        """
        self.r.close()

    def delete(self, key):
        """
        Delete key entry in Redis.
        """
        self.r.delete(key)

    def from_file(self, filename):
        """
        Restore redis DB from JSON file.
        """
        with open(filename, "r") as f:
            self.from_json(f.read())

    def from_json(self, json_str):
        """
        Restore redis DB from a JSON string
        """
        json_dict = json.loads(json_str)
        for k in json_dict.keys():
            self.r.set(k, json_dict[k])

    def get(self, key):
        """
        Return the value stored in "key" from Redis
        """
        return self.r.get(key).decode("utf-8")

    def get_keys(self, pattern):
        """
        Return list of Redis keys that match search pattern.
        """
        return [x.decode("utf-8") for x in self.r.keys(pattern)]

    def get_stats(self, key):
        """
        Return MRT stats from Redis as JSON, and return as an MRT stats object.
        """
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
        return self.r.get(key).decode("utf-8")

    def set_stats(self, key, mrt_s):
        """
        Take an MRT stats object, serialise it to JSON, store in Redis.
        """
        self.r.set(key, mrt_s.to_json())

    def set_stats_json(self, key, json_str):
        """
        Take JSON serialisation of an MRT stats object, and store in Redis.
        """
        self.r.set(key, json_str)

    def to_file(self, filename):
        """
        Dump the entire redis DB to a JSON file.
        """
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
