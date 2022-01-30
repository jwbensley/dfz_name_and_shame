
import sys
sys.path.append('./')
from redis_db import redis_db

def main():

    rdb = redis_db()
    running_stats = mrt_stats()
    now = datetime.datetime.now().strftime("%Y-%m-%d--%H-%m-%S")
        running_stats.merge_in(mrt_s)
        running_stats.timestamp = now

    rdb.set_stats(f"RV_LINX_RET:{now}", running_stats)

    global_stats = rdb.get_stats_global()
    if not global_stats.equal_to(running_stats):
        diff = global_stats.get_diff(running_stats)
        diff.timestamp = now
        rdb.set_stats(f"DIFF:{now}", diff)

        global_stats.merge_in(running_stats)
        rdb.set_stats_global(global_stats)
        print(f"Global stats updated")
    else:
        print("No update to global stats")

    rdb.close()

if __name__ == '__main__':
    main()