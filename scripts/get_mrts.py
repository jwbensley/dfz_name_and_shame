#!/usr/bin/env python3

import argparse
import logging
import os
import requests
import sys
import time

# Accomodate the use of the script, even when the dnas library isn't installed
sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)

from dnas.mrt_archives import mrt_archives
from dnas.config import config as cfg
from dnas.redis_db import redis_db
from dnas.mrt_getter import mrt_getter

def backfill(args):
    """
    Download any MRT which are missing from the stored stats, for a specific
    day.
    """
    if not args:
        raise ValueError(
            f"Missing required arguments: args={args}"
        )

    ymd = args["backfill"]
    rdb = redis_db()
    mrt_a = mrt_archives()

    for arch in mrt_a.archives:
        if (args["enabled"] and not arch.ENABLED):
            continue
        logging.debug(f"Archive {arch.NAME} is enabled")
        
        if args["rib"]:
            day_key = arch.gen_rib_key(ymd)
            day_stats = rdb.get_stats(day_key)

            all_filenames = arch.gen_rib_filenames(ymd)
            if day_stats:
                for filename_w_path in day_stats.file_list:
                    filename = os.path.basename(filename_w_path)
                    if filename in all_filenames:
                        all_filenames.remove(filename)

            if all_filenames:
                print(f"Need to backfill: {all_filenames}")
                for idx, filename in enumerate(all_filenames):
                    mrt_getter.download_mrt(
                        filename=arch.MRT_DIR + "/" + filename,
                        replace=args["replace"],
                        url=arch.gen_rib_url(filename=filename),
                    )
                    logging.info(f"Done {idx+1}/{len(all_filenames)}")

        if args["update"]:
            day_key = arch.gen_upd_key(ymd)
            day_stats = rdb.get_stats(day_key)

            all_filenames = arch.gen_upd_filenames(ymd)
            if day_stats:
                for filename_w_path in day_stats.file_list:
                    filename = os.path.basename(filename_w_path)
                    if filename in all_filenames:
                        all_filenames.remove(filename)

            if all_filenames:
                print(f"Need to backfill: {all_filenames}")
                for idx, filename in enumerate(all_filenames):
                    mrt_getter.download_mrt(
                        filename=arch.MRT_DIR + "/" + filename,
                        replace=args["replace"],
                        url=arch.gen_upd_url(filename=filename),
                    )
                    logging.info(f"Done {idx+1}/{len(all_filenames)}")

    rdb.close()

def continuous(args):
    """
    Continuous check for new MRT files and download them from the MRT archives
    enabled in the config.
    """
    if not args:
        raise ValueError(
            f"Missing required arguments: args={args}"
        )

    mrt_a = mrt_archives()

    while(True):
        for arch in mrt_a.archives:
            if (args["enabled"] and not arch.ENABLED):
                continue
            logging.debug(f"Archive {arch.NAME} is enabled")

            if args["rib"]:
                """
                In continuous mode we ignore HTTP erros like 404s.
                From time to time the MRT archives are unavailable, so any
                missed files will have to be backfiled later :(
                """
                try:
                    arch.get_latest_rib(
                        arch=arch,
                        replace=args["replace"],
                    )
                except requests.exceptions.HTTPError as e:
                    print(e)
                    pass

            if args["update"]:
                try:
                    arch.get_latest_upd(
                        arch=arch,
                        replace=args["replace"],
                    )
                except requests.exceptions.HTTPError as e:
                    print(e)
                    pass

        time.sleep(60)

def get_range(args):
    """
    Download a specific range of MRT files from all of the MRT archives which
    are enabled in the config.
    """
    if not args:
        raise ValueError(
            f"Missing required arguments: args={args}"
        )

    if (not args["start"] or not args["end"]):
        raise ValueError(
            "Both a '--start' and '--end' date are required for '--range'"
        )

    mrt_a = mrt_archives()

    for arch in mrt_a.archives:
        if (args["enabled"] and not arch.ENABLED):
            continue
        logging.debug(f"Archive {arch.NAME} is enabled")
            
        if args["rib"]:
            arch.get_range_rib(
                arch=arch,
                end_date=args["end"],
                replace=args["replace"],
                start_date=args["start"],
            )

        if args["update"]:
            arch.get_range_upd(
                arch=arch,
                end_date=args["end"],
                replace=args["replace"],
                start_date=args["start"],
            )

def parse_args():
    """
    Parse the CLI args to this script.
    """

    parser = argparse.ArgumentParser(
        description="Download MRT files from predefined source archives.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--backfill",
        help="Download any MRT files which are missing for a specific day. "
        "Specify the day in the MRT format yyyymmdd.",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument(
        "--continuous",
        help="Run in continuous mode - checking for new MRT files every "
        "minute and downloading them.",
        default=False,
        action="store_true",
        required=False,
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
        help="Only download MRT files for MRT archives enabled in the config.",
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
        "--latest",
        help="Download the latest MRT file. Use with --rib and/or --update.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--range",
        help="Download a range up files from --start to --end inclusive. "
        "Use with --rib and/or --update.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--replace",
        help="Replace/overwrite existing MRT files if they already exist.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--rib",
        help="Download RIB dump MRT files.",
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
        help="Download BGP update MRT files.",
        default=False,
        action="store_true",
        required=False,
    )

    return vars(parser.parse_args())

def main():

    args = parse_args()

    if args["debug"]:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=level)
    logging.info(f"Starting MRT downloader with logging level {level}")

    if not args["rib"] and not args["update"]:
        logging.error(
            "At least one of --rib and/or --update must be specified!"
        )
        exit(1)

    if args["backfill"]:
        backfill(args)

    if args["range"]:
        get_range(args)

    if args["continuous"]:
        continuous(args)

if __name__ == '__main__':
    main()