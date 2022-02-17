#!/usr/bin/env python3

import argparse
import datetime
import glob
import logging
import mrtparse
import multiprocessing
from multiprocessing import Pool
import os
import sys

# Accomodate the use of the script, even when the dnas library isn't installed
sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)

from dnas.config import config as cfg
from dnas.mrt_archives import mrt_archives
from dnas.mrt_stats import mrt_stats
from dnas.mrt_parser import mrt_parser
from dnas.mrt_splitter import mrt_splitter
from dnas.redis_db import redis_db

def parse_args():
    """
    Parse the CLI args to this script.
    """

    parser = argparse.ArgumentParser(
        description="Parse downloaded MRT files and store the stats in Redis.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--debug",
        help="Run with debug level logging.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--enabled",
        help="Only parse MRT files for MRT archives enabled in the config.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--rib",
        help="Parse RIB dump MRT files.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--single",
        help="Specify the path to a single MRT file to parse.",
        type=str,
        default=None,
        required=False,
    )
    parser.add_argument(
        "--remove",
        help="Delete MRT files once they have been prased.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--update",
        help="Parse BGP update MRT files.",
        default=False,
        action="store_true",
        required=False,
    )

    return vars(parser.parse_args())

def process_mrt_file(filename, remove):
    """
    Pass a single filename to the parser function.
    """
    if not filename:
        raise ValueError(
            f"Missing required arguments: filename={filename}"
        )

    mrt_a = mrt_archives()
    arch = mrt_a.arch_from_file_path(filename)
    if arch:
        process_files(filelist=[filename], remove=remove)
    else:
        exit(1)

def process_mrt_files(args):
    """
    Build the list of file to be parsed and pass them to the parser function.
    """
    if (not args):
        raise ValueError(
            f"Missing required arguments: args={args}"
        )

    mrt_a = mrt_archives()
    for arch in mrt_a.archives:
        if (args["enabled"] and not arch.ENABLED):
            continue

        logging.debug(f"Archive {arch.NAME} is enabled")

        if args["rib"]:
            glob_str = arch.MRT_DIR + arch.RIB_GLOB
            filelist = glob.glob(glob_str)
            process_files(filelist=filelist, remove=args["remove"])

        if args["update"]:
            glob_str = arch.MRT_DIR + arch.UPD_GLOB
            filelist = glob.glob(glob_str)
            process_files(filelist=filelist, remove=args["remove"])

def process_files(filelist, remove):
    """
    Parse all MRT files that match a file glob and store their parsed stats
    in redis.
    """
    if not filelist:
        raise ValueError(
            f"Missing required arguments: filelist={filelist}"
        )

    rdb = redis_db()
    mrt_a = mrt_archives()

    for file in filelist:
        logging.info(f"Checking file {file}")

        day_key = mrt_a.get_day_key(file)
        day_stats = rdb.get_stats(day_key)

        if day_stats:
            if file in day_stats.file_list:
                logging.info(f"Skipping {file}, already in {day_key}")
                continue

            mrt_s = process_file(file)
            day_stats.file_list.append(file)

            if day_stats.merge_in(mrt_s):
                logging.info(f"Updated {day_key} with {file}")
            else:
                logging.info(f"Added {file} to {day_key} file list")
            rdb.set_stats(day_key, day_stats)

        if not day_stats:
            mrt_s = process_file(file)
            mrt_s.file_list.append(file)
            rdb.set_stats(day_key, mrt_s)
            logging.info(f"Created new entry {day_key} from {file}")

        if remove:
            os.remove(file)
            logging.debug(f"Deleted {file}")

    rdb.close()

def process_file(filename=None, keep_chunks=False):
    """
    Split and parse an individual MRT file, return the mrt_stats.
    """

    if not filename:
        raise ValueError(
            f"Missing required arguments: filename={filename}."
        )

    no_cpu =  multiprocessing.cpu_count()
    Pool = multiprocessing.Pool(no_cpu)
    mrt_a = mrt_archives()

    logging.info(f"Processing {filename}...")

    splitter = mrt_splitter(filename)
    num_entries, file_chunks = splitter.split(no_cpu)
    try:
        splitter.close()
    except StopIteration:
        pass

    if mrt_a.is_rib_from_filename(filename):
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

    mrt_s.timestamp = mrt_parser.get_timestamp(filename)
    return mrt_s

def main():

    args = parse_args()

    if args["debug"]:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s', level=level
    )
    logging.info(f"Starting MRT parser with logging level {level}")

    if args["single"]:
        process_mrt_file(filename=args["single"], remove=args["remove"])
    else:
        process_mrt_files(args)

if __name__ == '__main__':
    main()