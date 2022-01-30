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
import glob
import logging
import mrtparse
import multiprocessing
from multiprocessing import Pool
import os

import sys
sys.path.append('./')
from config import config as cfg
from mrt_stats import mrt_stats
from mrt_getter import mrt_getter
from mrt_parser import mrt_parser
from mrt_splitter import mrt_splitter
from redis_db import redis_db


def old_main():

    # Download the RIB dump and MRT updates from 2 hours ago.
    #files = mrt_getter.get_latest_rv()
    #'/tmp/rib.20211222.1200.bz2',
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
        print(f"Global stats updated")
    else:
        print("No update to global stats")


def process_mrt_files(glob_str, rib_key, upd_key):

    rdb = redis_db()

    for file in glob.glob(glob_str):

        is_rib = True if ("rib" in file or "bview" in file) else False

        # Example: updates.20220101.0600.bz2 or updates.20220129.1145.gz
        day = file.split(".")[1]

        if is_rib:
            day_key = rib_key + ":" + day
        else:
            day_key = upd_key + ":" + day

        day_stats = rdb.get_stats(day_key)

        if day_stats:
            if file in day_stats.file_list:
                logging.info(f"Skipping {file}, already in {day_key}")
                continue

            mrt_s = process_file(file, is_rib)
            day_stats.file_list.append(file)

            if day_stats.merge_in(mrt_s):
                logging.info(f"Updated {day_key} with {file}")
            else:
                logging.info(f"Added {file} to {day_key} file list")
            rdb.set_stats(day_key, day_stats)

        if not day_stats:
            mrt_s = process_file(file, is_rib)
            mrt_s.file_list.append(file)
            rdb.set_stats(day_key, mrt_s)
            logging.info(f"Created new entry {day_key} from {file}")

        ######os.remove(file)

    rdb.close()

def process_file(filename, is_rib=False, keep_chunks=False):

    no_cpu =  multiprocessing.cpu_count()
    Pool = multiprocessing.Pool(no_cpu)

    logging.info(f"Processing {filename}...")

    splitter = mrt_splitter(filename)
    no_entries, file_chunks = splitter.split(no_cpu)
    try:
        splitter.close()
    except StopIteration:
        pass

    if is_rib:
        mrt_chunks = Pool.map(mrt_parser.parse_rib_dump, file_chunks)
    else:
        mrt_chunks = Pool.map(mrt_parser.parse_upd_dump, file_chunks)
    Pool.close()

    for i in range(0, len(file_chunks)):
        if not keep_chunks:
            os.remove(file_chunks[i])

    mrt_s = mrt_stats()
    for chunk in mrt_chunks:
        mrt_s.merge_in(chunk)
    _ = mrtparse.Reader(filename)
    ts = next(_).data["timestamp"][0]
    mrt_s.timestamp = datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d--%H-%M-%S')
    try:
        _.close()
    except StopIteration:
        pass

    return mrt_s


def main():

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    logging.info("Starting MRT parser")

    process_mrt_files(
        cfg.RV_LINX_GLOB,
        cfg.RV_LINX_RIB_KEY,
        cfg.RV_LINX_UPD_KEY,
    )

    exit(0)

    process_mrt_files(
        cfg.RV_SYDNEY_GLOB,
        cfg.RV_SYDNEY_RIB_KEY,
        cfg.RV_SYDNEY_UPD_KEY,
    )

    process_mrt_files(
        cfg.RCC_23_GLOB,
        cfg.RCC_23_RIB_KEY,
        cfg.RCC_23_UPD_KEY,
    )

    process_mrt_files(
        cfg.RCC_24_GLOB,
        cfg.RCC_24_RIB_KEY,
        cfg.RCC_24_UPD_KEY,
    )

if __name__ == '__main__':
    main()