import sys
sys.path.append('./')
from redis_db import redis_db

rdb = redis_db()

def wipe_and_restore(filename):
    for k in rdb.get_keys("*"):
        rdb.delete(k)
    rdb.from_file(filename)

def backup(filename):
    rdb.to_file(filename)

def main():

    backup("redis_dump.json")
    #wipe_and_restore("redis_dump.json")

    rdb.close()

if __name__ == '__main__':
    main()