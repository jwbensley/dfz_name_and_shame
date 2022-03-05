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
from dnas.mrt_archive import mrt_archive
from dnas.mrt_stats import mrt_stats
from dnas.mrt_parser import mrt_parser
from dnas.mrt_splitter import mrt_splitter
from dnas.redis_db import redis_db

def parse_args():
    """
    Parse the CLI args to this script.
    """
    parser = argparse.ArgumentParser(
        description="Parse downloaded MRT files and store the stats in Redis. "
        "Specific MRT files will be parsed if either --range, --ymd, or "
        "--single is used. If none of these are specified, all downloaded MRT "
        "files will be parsed.",
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
        "--end",
        help="End date in format 'yyyymmdd.hhmm' e.g., '20220101.2359'.",
        type=str,
        required=False,
        default=None,
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
        "--range",
        help="Parse a range up files from --start to --end inclusive. "
        "Use with --rib and/or --update.",
        default=False,
        action="store_true",
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
        "--start",
        help="Start date in format 'yyyymmdd.hhmm' e.g., '20220101.0000'.",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument(
        "--update",
        help="Parse BGP update MRT files.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--ymd",
        help="Specify a day to parse all MRT files from that specific day. "
        "Must use yyyymmdd format e.g., 20220101.",
        type=str,
        default=None,
        required=False,
    )

    return vars(parser.parse_args())

def parse_file(filename=None, keep_chunks=False):
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
        mrt_s.add(chunk)

    return mrt_s

def parse_files(filelist, remove):
    """
    A wrapper around the single file parsing function parse_file(), which
    accepts a list of files to parse.
    """
    if not filelist:
        raise ValueError(
            f"Missing required arguments: filelist={filelist}"
        )

    rdb = redis_db()
    mrt_a = mrt_archives()

    logging.info(f"Done 0/{len(filelist)}")
    for idx, file in enumerate(filelist):
        logging.info(f"Checking file {file}")

        day_key = mrt_a.get_day_key(file)
        day_stats = rdb.get_stats(day_key)

        if day_stats:
            if file in day_stats.file_list:
                logging.info(f"Skipping {file}, already in {day_key}")
                continue

            mrt_s = parse_file(file)
            if day_stats.add(mrt_s):
                logging.info(f"Added {file} to {day_key}")
            else:
                logging.info(f"Added {file} to {day_key} file list")
            rdb.set_stats(day_key, day_stats)

        if not day_stats:
            mrt_s = parse_file(file)
            rdb.set_stats(day_key, mrt_s)
            logging.info(f"Created new entry {day_key} from {file}")

        if remove:
            os.remove(file)
            logging.debug(f"Deleted {file}")

        logging.info(f"Done {idx+1}/{len(filelist)}")

    rdb.close()

def process_day(args):
    """
    Build the list of files to be parsed and pass them to the parser function.
    This function builds a list MRT files from a specific day, from eligble MRT
    archives.
    """
    if (not args):
        raise ValueError(
            f"Missing required arguments: args={args}"
        )

    if (not args["ymd"]):
        raise ValueError(
            f"Missing required arguments: ymd={args['ymd']}"
        )

    if (not args["rib"] and not args["update"]):
        raise ValueError(
            "At least one of --rib and/or --update must be specified with "
            "--ymd"
        )

    mrt_archive.valid_ymd(args["ymd"])
    mrt_a = mrt_archives()
    filelist = []
    for arch in mrt_a.archives:
        if (args["enabled"] and not arch.ENABLED):
            continue
        logging.debug(f"Archive {arch.NAME} is enabled")

        if args["rib"]:
            glob_str = arch.MRT_DIR + arch.RIB_PREFIX + "*" + args["ymd"] + "*"
            filelist.extend(glob.glob(glob_str))

        if args["update"]:
            glob_str = arch.MRT_DIR + arch.UPD_PREFIX + "*" + args["ymd"] + "*"
            filelist.extend(glob.glob(glob_str))

    if not filelist:
        logging.info(f"No files found to process for this day")
        return

    parse_files(filelist=filelist, remove=args["remove"])

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
        parse_files(filelist=[filename], remove=remove)
    else:
        exit(1)

def process_mrt_glob(args):
    """
    Build the list of files to be parsed based on a file glob, then pass them
    to the parser function. This function builds a list of all available MRT
    files from all eligble MRT archives.
    """
    if (not args):
        raise ValueError(
            f"Missing required arguments: args={args}"
        )

    mrt_a = mrt_archives()
    filelist = []
    for arch in mrt_a.archives:
        if (args["enabled"] and not arch.ENABLED):
            continue
        logging.debug(f"Archive {arch.NAME} is enabled")

        if args["rib"]:
            glob_str = arch.MRT_DIR + arch.RIB_GLOB
            filelist.extend(glob.glob(glob_str))

        if args["update"]:
            glob_str = arch.MRT_DIR + arch.UPD_GLOB
            filelist.extend(glob.glob(glob_str))
    
    parse_files(filelist=filelist, remove=args["remove"])

def process_range(args):
    """
    Build a list of MRT files between the --start and --end dates inclusive
    to pass to the MRT parser function.
    """
    if (not args):
        raise ValueError(
            f"Missing required arguments: args={args}"
        )

    if (not args["start"] and not args["end"]):
        raise ValueError(
            "Both --start and --end must be specified when using --range"
        )


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
    elif args["ymd"]:
        process_day(args)
    elif args["range"]:
        process_range(args)
    else:
        process_mrt_glob(args)

if __name__ == '__main__':
    main()