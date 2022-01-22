"""
Requires:
pypy3.8-v7.3.7

pypy3 -mpip ensurepip
pypy3 -mpip install --upgrade pip
pypy3 -mpip install mrtparse
pypy3 -mpip install requests
pypy3 -mpip install redis
"""

import datetime
import mrtparse
import multiprocessing
from multiprocessing import Pool
import os
import sys
sys.path.append('./')
from mrt_stats import mrt_stats
from mrt_getter import mrt_getter
from mrt_parser import mrt_parser
from mrt_splitter import mrt_splitter
from redis_db import redis_db


def main():

    # Download the RIB dump and MRT updates from 2 hours ago.
    #files = mrt_getter.get_latest_rv()
    files = ['/tmp/rib.20211222.1200.bz2', '/tmp/updates.20211222.1200.bz2', '/tmp/updates.20211222.1215.bz2', '/tmp/updates.20211222.1230.bz2', '/tmp/updates.20211222.1245.bz2', '/tmp/updates.20211222.1300.bz2', '/tmp/updates.20211222.1315.bz2', '/tmp/updates.20211222.1330.bz2', '/tmp/updates.20211222.1345.bz2']
    num_procs =  multiprocessing.cpu_count()
    Pool = multiprocessing.Pool(num_procs)
    rdb = redis_db()
    running_stats = mrt_stats()
    now = datetime.datetime.now().strftime("%Y-%m-%d--%H-%m-%S")

    for file in files:

        print(f"Processing file {file}...")

        splitter = mrt_splitter(file)
        no_entries, file_chunks = splitter.split(num_procs)


        rib = True if ("rib" in file) else False
        if rib:
            mrt_chunks = Pool.map(mrt_parser.parse_rib_dump, file_chunks)
        else:
            mrt_chunks = Pool.map(mrt_parser.parse_upd_dump, file_chunks)

        mrt_s = mrt_stats()
        for chunk in mrt_chunks:
            mrt_s.merge_in(chunk)
        _ = mrtparse.Reader(file)
        ts = next(_).data["timestamp"][0]
        mrt_s.timestamp = datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d--%H-%M-%S')

        if rib:
            rdb.set_stats(f"RV_LINX_RIB:{os.path.basename(file)}", mrt_s)
        else:
            rdb.set_stats(f"RV_LINX_UPD:{os.path.basename(file)}", mrt_s)

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
    else:
        print("No update to global stats")


if __name__ == '__main__':
    main()