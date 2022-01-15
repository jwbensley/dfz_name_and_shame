import redis
import sys
sys.path.append('./')
from redis_auth import redis_auth
from mrt_stats import mrt_stats

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

        """
        Perform some initial setup if this is a new/blank RedisDB.
        """
        if not self.r.exists("GLOBAL"):
            blank_stats = mrt_stats()
            self.set_stats("GLOBAL", blank_stats)

    def get(self, key):
        """
        Return the value stored in "key" from Redis
        """
        return self.r.get(key).decode("utf-8")

    def get_stats(self, key):
        """
        Return MRT stats from Redis as JSON, and return as an MRT stats object.
        """
        mrt_s = mrt_stats()
        mrt_s.from_json(self.r.get(key).decode("utf-8"))
        return mrt_s

    def get_stats_global(self):
        """
        Return the last global stats data.
        """
        return self.get_stats("GLOBAL")

    def get_stats_json(self, key):
        """
        Return MRT stats from Redis as JSON string.
        """
        return self.r.get(key).decode("utf-8")

    def set_stats(self, key, mrt_s):
        """
        Take an MRT stats object, serialisesit to JSON, store in Redis.
        """
        self.r.set(key, mrt_s.to_json())

    def set_stats_global(self, mrt_s):
        """
        Set the global stats data.
        """
        self.set_stats("GLOBAL", mrt_s)

    def set_stats_json(self, key, json_str):
        """
        Take JSON serialisation of an MRT stats object, and store in Redis.
        """
        self.r.set(key, json_str)

