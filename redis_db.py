import redis
import sys
sys.path.append('./')
from redis_auth import redis_auth

r = redis.Redis(
	host=redis_auth.host,
	port=redis_auth.port
)

r.set("baz", "bar")
print(r.get("baz"))
print(r.get("my_key"))
