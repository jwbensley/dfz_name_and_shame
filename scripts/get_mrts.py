#!/usr/bin/env python3

import argparse
import datetime
import logging
import os
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

def continuous(args):
    """
    Continuous check for new MRT files and download them from the MRT archives
    enabled in the config.
    """

    mrt_a = mrt_archives()

    while(True):
        for arch in mrt_a.archives:
            if arch.ENABLED:
                logging.debug(f"Archive {arch.NAME} is enabled")

            if args["rib"]:
                arch.get_latest_rib(
                    base_url=arch.BASE_URL,
                    mrt_dir=arch.MRT_DIR,
                    file_ext=arch.MRT_EXT,
                    replace=args["replace"],
                    rib_url=arch.UPD_URL,
                )

            if args["update"]:
                arch.get_latest_upd(
                    base_url=arch.BASE_URL,
                    mrt_dir=arch.MRT_DIR,
                    file_ext=arch.MRT_EXT,
                    replace=args["replace"],
                    upd_url=arch.UPD_URL,
                )

        time.sleep(60)

def parse_args():
    """
    Parse the CLI args to this script.
    """

    parser = argparse.ArgumentParser(
        description="Download MRT files from predefined source archives.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
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

def range(args):
    """
    Download a specific range of MRT files from al of the MRT archives which
    are enabled in the config.
    """

    if (not args["start"] or not args["end"]):
        print("Both a '--start' and '--end' date are required for '--range'")
        exit(1)

    mrt_a = mrt_archives()

    for arch in mrt_a.archives:

        if arch.ENABLED:
            logging.debug(f"Archive {arch.NAME} is enabled")
            
            if args["rib"]:
                arch.get_range_rib(
                    base_url=arch.BASE_URL,
                    mrt_dir=arch.MRT_DIR,
                    end_date=args["end"],
                    file_ext=arch.MRT_EXT,
                    replace=args["replace"],
                    rib_url=arch.UPD_URL,
                    start_date=args["start"],
                )

            if args["update"]:
                arch.get_range_upd(
                    base_url=arch.BASE_URL,
                    mrt_dir=arch.MRT_DIR,
                    end_date=args["end"],
                    file_ext=arch.MRT_EXT,
                    replace=args["replace"],
                    start_date=args["start"],
                    upd_url=arch.UPD_URL,
                )

def main():

    args = parse_args()

    if args["debug"]:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=level)
    logging.info(f"Starting MRT downloader with logging level {level}")

    if args["range"]:
        range(args)

    if args["continuous"]:
        continuous(args)

if __name__ == '__main__':
    main()