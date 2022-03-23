#!/usr/bin/env python3

import argparse
import datetime
import logging
import os
import requests
import sys
import time
from typing import Any, Dict, List

# Accomodate the use of the dnas library, even when the library isn't installed
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

def continuous(args: Dict[str, Any] = None):
    """
    Continuous check for new MRT files and download them from the MRT archives
    enabled in the config.
    """
    if not args:
        raise ValueError(
            f"Missing required arguments: args={args}"
        )

    if type(args) != dict:
        raise TypeError(
            f"args is not a dict: {type(args)}"
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
                missed files will have to be manually backfilled later.
                """
                try:
                    mrt_getter.get_latest_rib(
                        arch=arch,
                        replace=args["replace"],
                    )
                except requests.exceptions.HTTPError as e:
                    logging.error(e)
                    pass

            if args["update"]:
                try:
                    mrt_getter.get_latest_upd(
                        arch=arch,
                        replace=args["replace"],
                    )
                except requests.exceptions.HTTPError as e:
                    logging.error(e)
                    pass

        time.sleep(60)

def get_mrts(replace: bool = False, url_list: List[str] = None):
    """
    Download the list of MRTs from the passed URL list.
    """
    if not url_list:
        raise ValueError(
            f"Missing required arguments: url_list={url_list}"
        )

    if type(url_list) != list:
        raise TypeError(
            f"url_list is not a list: {type(url_list)}"
        )

    mrt_a = mrt_archives()
    logging.info(f"Downloading {len(url_list)} MRT files")
    i = 0
    for url in url_list:
        arch = mrt_a.arch_from_url(url)
        if not arch:
            raise ValueError(f"Couldn't match {url} to any MRT archive")
        outfile = os.path.normpath(arch.MRT_DIR + "/" + os.path.basename(url))
        """
        When downloading a large range of updates, say an entire month for
        example, some may be missing because the BGP collector had an
        outage. For this reason, ignore HTTP erros like 404s.
        """
        try:
            mrt_getter.download_mrt(
                filename=outfile,
                replace=replace,
                url=url
            )
            i += 1
            logging.info(f"Downloaded {i}/{len(url_list)}")
        except requests.exceptions.HTTPError as e:
            logging.error(e)
            pass

    logging.info(f"Finished, downloaded {i}/{len(url_list)}")

def get_day(args: Dict[str, Any] = None):
    """
    Download all the MRTs for a specific day.
    """
    if not args:
        raise ValueError(
            f"Missing required arguments: args={args}"
        )

    if not args["ymd"]:
        raise ValueError(
            f"Missing required arguments: ymd={args['ymd']}"
        )

    url_list = gen_urls_day(args)
    if not url_list:
        logging.info("Nothing to download")
    else:
        get_mrts(replace=args["replace"], url_list=url_list)

def get_range(args: Dict[str, Any] = None):
    """
    Download all the MRTs for between a specific start and end date inclusive.
    """
    if not args:
        raise ValueError(
            f"Missing required arguments: args={args}"
        )

    if (not args["start"] or not args["end"]):
        raise ValueError(
            f"Missing required options: start={args['start']}, "
            f"end={args['end']}"
        )

    url_list = gen_urls_range(args)
    if not url_list:
        logging.info("Nothing to download")
    else:
        get_mrts(replace=args["replace"], url_list=url_list)

def gen_urls_day(args: Dict[str, Any] = None) -> List[str]:
    """
    Return a list of URLs for all the MRTs for a specific day.
    """
    if not args:
        raise ValueError(
            f"Missing required arguments: args={args}"
        )

    if not args["ymd"]:
        raise ValueError(
            f"Missing required arguments: ymd={args['ymd']}"
        )

    args["start"] = args["ymd"] + ".0000"
    args["end"] = args["ymd"] + ".2359"
    return gen_urls_range(args)

def gen_urls_range(args: Dict[str, Any] = None) -> List[str]:
    """
    Generate and return a list of URLs for all MRTs betwen a start and end date
    inclusive.
    """
    if not args:
        raise ValueError(
            f"Missing required arguments: args={args}"
        )

    if (not args["start"] or not args["end"]):
        raise ValueError(
            f"Missing required options: start={args['start']}, "
            f"end={args['end']}"
        )

    start = datetime.datetime.strptime(args["start"], cfg.TIME_FORMAT)
    end = datetime.datetime.strptime(args["end"], cfg.TIME_FORMAT)

    if end < start:
        raise ValueError(
            f"End date {end} is before start date {start}"
        )

    diff = end - start
    no_days = int(diff.total_seconds() // 86400)
    rdb = redis_db()
    mrt_a = mrt_archives()
    url_list = []

    for i in range(0, no_days + 1):

        delta = datetime.timedelta(days=i)
        ymd = datetime.datetime.strftime(start + delta, cfg.DAY_FORMAT)

        for arch in mrt_a.archives:
            if (args["enabled"] and not arch.ENABLED):
                continue
            logging.debug(f"Checking archive {arch.NAME}...")

            if args["rib"]:

                all_rib_filenames = arch.gen_rib_fns_day(ymd)

                for filename in all_rib_filenames[:]:
                    raw_ts = '.'.join(filename.split(".")[1:3])
                    timestamp = datetime.datetime.strptime(
                        raw_ts, cfg.TIME_FORMAT
                    )
                    if (timestamp < start or timestamp > end):
                        all_rib_filenames.remove(filename)

                if not all_rib_filenames:
                    continue

                """
                if we are only downloading what is not already in the DB, pull
                the stats for this day and check which files are missing.
                """
                if args["backfill"]:
                    day_key = arch.gen_rib_key(ymd)
                    day_stats = rdb.get_stats(day_key)

                    if day_stats:
                        for filename_w_path in day_stats.file_list:
                            filename = os.path.basename(filename_w_path)
                            if filename in all_rib_filenames:
                                all_rib_filenames.remove(filename)

                    if all_rib_filenames:
                        logging.info(
                            f"Need to backfill {len(all_rib_filenames)} RIB "
                            f"dumps for archive {arch.NAME} on {ymd}"
                        )
                        urls = [arch.gen_rib_url(filename) for filename in all_rib_filenames]
                        logging.debug(f"Adding {urls}")
                        url_list.extend(urls)
                    else:
                        logging.info(
                            f"No files needed to backfill RIB dumps for "
                            f"archive {arch.NAME} on {ymd}"
                        )

                else:
                    """
                    Else, download files regardless of whether their stats are
                    already in the DB.
                    """
                    logging.info(
                        f"Adding {len(all_rib_filenames)} RIB dumps for "
                        f"archive {arch.NAME} on {ymd}"
                    )
                    urls = [arch.gen_rib_url(filename) for filename in all_rib_filenames]
                    logging.debug(f"Adding {urls}")
                    url_list.extend(urls)

            if args["update"]:

                all_upd_filenames = arch.gen_upd_fns_day(ymd)

                for filename in all_upd_filenames[:]:
                    raw_ts = '.'.join(filename.split(".")[1:3])
                    timestamp = datetime.datetime.strptime(
                        raw_ts, cfg.TIME_FORMAT
                    )
                    if (timestamp < start or timestamp > end):
                        all_upd_filenames.remove(filename)

                if not all_upd_filenames:
                    continue

                if args["backfill"]:
                    day_key = arch.gen_upd_key(ymd)
                    day_stats = rdb.get_stats(day_key)

                    if day_stats:
                        for filename_w_path in day_stats.file_list:
                            filename = os.path.basename(filename_w_path)
                            if filename in all_upd_filenames:
                                all_upd_filenames.remove(filename)

                    if all_upd_filenames:
                        logging.info(
                            f"Need to backfill {len(all_upd_filenames)} UPDATE "
                            f"dumps for archive {arch.NAME} on {ymd}"
                        )
                        urls = [arch.gen_upd_url(filename) for filename in all_upd_filenames]
                        logging.debug(f"Adding {urls}")
                        url_list.extend(urls)
                    else:
                        logging.info(
                            f"No files needed to backfill UPDATE dumps for "
                            f"archive {arch.NAME} on {ymd}"
                        )

                else:
                    logging.info(
                        f"Adding {len(all_upd_filenames)} UPDATE dumps for "
                        f"archive {arch.NAME} on {ymd}"
                    )
                    urls = [arch.gen_upd_url(filename) for filename in all_upd_filenames]
                    logging.debug(f"Adding {urls}")
                    url_list.extend(urls)

    rdb.close()
    return url_list

def parse_args():
    """
    Parse the CLI args to this script.
    """
    parser = argparse.ArgumentParser(
        description="Download MRT files from public MRT archives. "
        "One of three modes must be chosen: --continuous, --range, or --ymd. "
        "One or both of --rib and --update must be chosen.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--backfill",
        help="Only download files if they are missing from stats entries stored"
        "in Redis.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--continuous",
        help="Run in continuous mode - download the latest MRT file from each "
        "archive as it becomes available. Use with --rib and/or --update.",
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
        help="Download the single latest MRT file. Use with --rib and/or "
        "--update.",
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
    parser.add_argument(
        "--ymd",
        help="Specify a day to download all MRT files from, for that specific "
        "day. Must use yyyymmdd format e.g., 20220101.",
        type=str,
        default=None,
        required=False,
    )

    return vars(parser.parse_args())

def main():

    args = parse_args()

    os.makedirs(os.path.dirname(cfg.LOG_DIR), exist_ok=True)
    if args["debug"]:
        logging.basicConfig(
            format='%(asctime)s %(levelname)s %(funcName)s %(message)s',
            level=logging.DEBUG,
            handlers=[
                logging.FileHandler(cfg.LOG_GETTER, mode=cfg.LOG_MODE),
                logging.StreamHandler()
            ]
        )
    else:
        logging.basicConfig(
            format='%(asctime)s %(levelname)s %(message)s',
            level=logging.INFO,
            handlers=[
                logging.FileHandler(cfg.LOG_GETTER, mode=cfg.LOG_MODE),
                logging.StreamHandler()
            ]
        )

    logging.info(
        f"Starting MRT downloader with logging level "
        f"{logging.getLevelName(logging.getLogger().getEffectiveLevel())}"
    )

    if not args["continuous"] and not args["range"] and not args["ymd"]:
        logging.error(
            "Exactly one of the three modes must be chosen: --continuous, "
            "--range, or --ymd!"
        )
        exit(1)

    if not args["rib"] and not args["update"]:
        logging.error(
            "At least one of --rib and/or --update must be specified!"
        )
        exit(1)

    if args["continuous"]:
        continuous(args)
    elif args["range"]:
        get_range(args)
    elif args["ymd"]:
        get_day(args)

if __name__ == '__main__':
    main()