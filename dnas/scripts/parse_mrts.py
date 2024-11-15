#!/usr/bin/env python3

import argparse
import datetime
import glob
import logging
import multiprocessing
import os
import sys
import time
from multiprocessing import Pool

# Accommodate the use of the dnas library, even when the library isn't installed
sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
)

from dnas.config import config as cfg
from dnas.log import log
from dnas.mrt_archive import mrt_archive
from dnas.mrt_archives import mrt_archives
from dnas.mrt_parser import mrt_parser
from dnas.mrt_splitter import MrtFormatError, mrt_splitter
from dnas.mrt_stats import mrt_stats
from dnas.redis_db import redis_db


def continuous(args: dict) -> None:
    """
    Continuously parse new MRT files as they are download from the configured
    MRT archives. This function simply globs for all MRTs that match todays
    "ymd" value. This is problematic around midnight e.g., an MRT for 23.50 to
    00.00 might not be available until 00.05 the next day. This function sets
    the "ymd" value to be now() - $delta minutes, meaning it globs for files
    from the previous day up until $delta past midnight each day.
    """
    if not args:
        raise ValueError(f"Missing required arguments: args={args}")

    if type(args) != dict:
        raise TypeError(f"args is not a dict: {type(args)}")

    mrt_a = mrt_archives()
    min_interval = cfg.DFT_INTERVAL

    while True:
        delta = datetime.timedelta(minutes=90)
        glob_ymd = datetime.datetime.strftime(
            datetime.datetime.utcnow() - delta, cfg.DAY_FORMAT
        )
        logging.debug(f"Glob ymd is {glob_ymd}")

        filelist = []
        for arch in mrt_a.archives:
            if args["enabled"] and not arch.ENABLED:
                continue
            logging.debug(f"Archive {arch.NAME} is enabled")

            if args["rib"]:
                glob_str = (
                    str(arch.MRT_DIR + "/")
                    .replace("///", "/")
                    .replace("//", "/")
                )
                glob_str += arch.RIB_PREFIX + "*" + glob_ymd + "*"
                filelist.extend(glob.glob(glob_str))

                """
                Only check for new MRTs to parse as frequently as the MRT archive
                which provides the most frequent dumps:
                """
                if (arch.RIB_INTERVAL * 60) < min_interval:
                    min_interval = arch.RIB_INTERVAL * 60
                    logging.debug(
                        f"Parse interval set to {min_interval} by "
                        f"{arch.NAME} RIB interval"
                    )

            if args["update"]:
                glob_str = (
                    str(arch.MRT_DIR + "/")
                    .replace("///", "/")
                    .replace("//", "/")
                )
                glob_str += arch.UPD_PREFIX + "*" + glob_ymd + "*"
                filelist.extend(glob.glob(glob_str))

                if (arch.UPD_INTERVAL * 60) < min_interval:
                    min_interval = arch.UPD_INTERVAL * 60
                    logging.debug(
                        f"Parse interval set to {min_interval} by "
                        f"{arch.NAME} UPD interval"
                    )

        if filelist:
            logging.debug(f"Checking for {len(filelist)} files: {filelist}")
            parse_files(filelist=filelist, args=args)

        time.sleep(min_interval)


def parse_args() -> dict:
    """
    Parse the CLI args to this script.
    """
    parser = argparse.ArgumentParser(
        description="Parse downloaded MRT files and store the stats in Redis.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    action_group = parser.add_argument_group(
        "Action", "Method for getting MRTs"
    )
    actions = action_group.add_mutually_exclusive_group(required=True)
    actions.add_argument(
        "--continuous",
        help="Run in continuous mode - parse available MRT files from each "
        "archive as they are downloaded.",
        default=False,
        action="store_true",
        required=False,
    )
    actions.add_argument(
        "--range",
        help="Parse a range of files from --start to --end inclusive.",
        default=False,
        action="store_true",
        required=False,
    )
    actions.add_argument(
        "--single",
        help="Specify the path to a single MRT file to parse.",
        type=str,
        default=None,
        required=False,
    )
    actions.add_argument(
        "--yesterday",
        help="This is a shortcut for --ymd yyyymmdd using yesterdays date.",
        default=False,
        action="store_true",
        required=False,
    )
    actions.add_argument(
        "--ymd",
        help="Specify a day to parse all MRT files from that specific day. "
        "Must use yyyymmdd format e.g., 20220101.",
        type=str,
        default=None,
        required=False,
    )

    type_group = parser.add_argument_group(
        "MRT Type", "Type of MRT file(s) to parse"
    )
    types = type_group.add_mutually_exclusive_group(required=True)
    types.add_argument(
        "--rib",
        help="Parse RIB dump MRT files.",
        default=False,
        action="store_true",
        required=False,
    )
    types.add_argument(
        "--update",
        help="Parse BGP update MRT files.",
        default=False,
        action="store_true",
        required=False,
    )

    filter_group = parser.add_argument_group(
        "MRT Filters", "Filter which MRT file(s) to parse"
    )
    filter_group.add_argument(
        "--enabled",
        help="Only parse MRT files for MRT archives enabled in the config.",
        default=False,
        action="store_true",
        required=False,
    )
    filter_group.add_argument(
        "--end",
        help="End date in format 'yyyymmdd.hhmm' e.g., '20220101.2359'. "
        "For use with --range.",
        type=str,
        required=False,
        default=None,
    )
    filter_group.add_argument(
        "--start",
        help="Start date in format 'yyyymmdd.hhmm' e.g., '20220101.0000'. "
        "For use with --range.",
        type=str,
        required=False,
        default=None,
    )

    parser.add_argument(
        "--debug",
        help="Run with debug level logging.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--no-multi",
        help="Run in single process mode only, don't use all CPU cores which is"
        " the default behaviour.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--overwrite",
        help="Parse files even if they have already been parsed before and "
        "their stats entries are already stored in Redis.",
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

    return vars(parser.parse_args())


def parse_file(
    filename: str, keep_chunks: bool = False, multi: bool = True
) -> "mrt_stats":
    """
    Split and parse an individual MRT file, return the mrt_stats.
    """
    if not filename:
        raise ValueError(f"Missing required arguments: filename={filename}.")

    if type(filename) != str:
        raise TypeError(f"filename is not a string: {type(filename)}")

    mrt_a = mrt_archives()
    logging.info(f"Processing {filename}...")

    fs = os.path.getsize(filename)
    if fs > cfg.MAX_MRT_SIZE:
        logging.warning(
            f"File size of {filename} ({(fs/ 1000 / 1000):0.4}MBs) is greater "
            f"than max size ({cfg.MAX_MRT_SIZE}MB), forcing single process "
            "parsing"
        )
        multi = False
    elif fs < 64:
        logging.error(
            f"Skipping file {filename}. File size ({fs} bytes) is less "
            f"than the minimum required size ({cfg.MIN_MRT_SIZE}). This is "
            f"assumed to be an invalid file."
        )
        return mrt_stats()

    if multi:
        no_cpu = multiprocessing.cpu_count()
        Pool = multiprocessing.Pool(no_cpu)

        splitter = mrt_splitter(filename)
        num_entries, file_chunks = splitter.split(
            no_chunks=no_cpu, outdir=cfg.SPLIT_DIR
        )
        try:
            splitter.close()
        except StopIteration:
            pass

        if mrt_a.is_rib_from_filename(filename):
            mrt_chunks = Pool.map(mrt_parser.parse_rib_dump, file_chunks)
        else:
            mrt_chunks = Pool.map(mrt_parser.parse_upd_dump, file_chunks)

        Pool.close()

        if not keep_chunks:
            for i in range(0, len(file_chunks)):
                if cfg.SPLIT_DIR:
                    os.remove(
                        os.path.join(
                            cfg.SPLIT_DIR, os.path.basename(file_chunks[i])
                        )
                    )
                else:
                    os.remove(file_chunks[i])

        mrt_s = mrt_stats()
        for chunk in mrt_chunks:
            mrt_s.add(chunk)

    else:
        if mrt_a.is_rib_from_filename(filename):
            mrt_s = mrt_parser.parse_rib_dump(filename)
        else:
            mrt_s = mrt_parser.parse_upd_dump(filename)

    return mrt_s


def parse_files(filelist: list[str], args: dict) -> None:
    """
    A wrapper around the single file parsing function parse_file(), which
    accepts a list of files to parse.
    """
    if not filelist or not args:
        raise ValueError(
            f"Missing required arguments: filelist={filelist}, args={args}"
        )

    if type(filelist) != list:
        raise TypeError(f"filelist is not a list: {type(filelist)}")

    rdb = redis_db()
    mrt_a = mrt_archives()

    logging.info(f"Done 0/{len(filelist)}")
    for idx, file in enumerate(filelist):
        logging.info(f"Checking file {file}")

        arch = mrt_a.arch_from_file_path(file)
        day_key = mrt_a.get_day_key(file)
        day_stats = rdb.get_stats(day_key)

        if day_stats:
            if file in day_stats.file_list and not args["overwrite"]:
                logging.info(f"Skipping {file}, already in {day_key}")
                if args["remove"]:
                    logging.debug(f"Deleting {file}")
                    os.remove(file)
                continue

        try:
            mrt_s = parse_file(filename=file, multi=args["multi"])
        except MrtFormatError as e:
            logging.error(
                f"Couldn't parse file {file} due to formatting error: "
                f"{str(e)}"
            )
            continue
        except EOFError as e:
            logging.error(f"Unable to split {file}, unexpected EOF: {e}")
            os.remove(file)
            logging.error(f"Deleted {file}")
            continue

        if day_stats:
            if day_stats.add(mrt_s):
                if arch:
                    day_stats.add_archive(arch.NAME)
                else:
                    logging.warning(
                        f"Unable to add archive name to stats object"
                    )
                logging.info(f"Added {file} to {day_key}")
            elif file not in day_stats.file_list:
                logging.info(f"Added {file} to {day_key} file list")
                day_stats.file_list.append(file)
            rdb.set(day_key, day_stats.to_json())

        else:
            if arch:
                mrt_s.add_archive(arch.NAME)
            else:
                logging.warning(f"Unable to add archive name to stats object")
            rdb.set(day_key, mrt_s.to_json())
            logging.info(f"Created new entry {day_key} from {file}")

        if args["remove"]:
            logging.debug(f"Deleting {file}")
            os.remove(file)

        logging.info(f"Done {idx+1}/{len(filelist)}")

    rdb.close()


def process_day(args: dict) -> None:
    """
    Build the list of files to be parsed and pass them to the parser function.
    This function builds a list MRT files from a specific day, from eligble MRT
    archives.
    """
    if not args:
        raise ValueError(f"Missing required arguments: args={args}")

    if not args["ymd"]:
        raise ValueError(f"Missing required arguments: ymd={args['ymd']}")

    mrt_archive.valid_ymd(args["ymd"])
    mrt_a = mrt_archives()
    filelist = []
    for arch in mrt_a.archives:
        if args["enabled"] and not arch.ENABLED:
            continue
        logging.debug(f"Checking archive {arch.NAME}...")

        if args["rib"]:
            glob_str = arch.MRT_DIR + arch.RIB_PREFIX + "*" + args["ymd"] + "*"
            filelist.extend(glob.glob(glob_str))

        if args["update"]:
            glob_str = arch.MRT_DIR + arch.UPD_PREFIX + "*" + args["ymd"] + "*"
            filelist.extend(glob.glob(glob_str))

    if not filelist:
        logging.info(f"No files found to process for this day")
        return

    parse_files(filelist=filelist, args=args)


def process_mrt_file(args: dict) -> None:
    """
    Pass a single filename to the parser function.
    """
    if not args:
        raise ValueError(f"Missing required arguments: args={args}")

    filename = args["single"]

    if type(filename) != str:
        raise TypeError(f"filename is not a string: {type(filename)}")

    mrt_a = mrt_archives()
    arch = mrt_a.arch_from_file_path(filename)
    # Check that this file can be matched to a known MRT archive:
    if arch:
        parse_files(filelist=[filename], args=args)
    else:
        exit(1)


def process_mrt_glob(args: dict) -> None:
    """
    Build the list of files to be parsed based on a file glob, then pass them
    to the parser function. This function builds a list of all available MRT
    files from all eligble MRT archives.
    """
    if not args:
        raise ValueError(f"Missing required arguments: args={args}")

    if type(args) != dict:
        raise TypeError(f"args is not a dict: {type(args)}")

    mrt_a = mrt_archives()
    filelist = []
    for arch in mrt_a.archives:
        if args["enabled"] and not arch.ENABLED:
            continue
        logging.debug(f"Checking archive {arch.NAME}...")

        if args["rib"]:
            glob_str = arch.MRT_DIR + arch.RIB_GLOB
            glob_files = glob.glob(glob_str)
            logging.debug(f"Adding {len(glob_files)} from archive {arch.NAME}")
            filelist.extend(glob_files)

        if args["update"]:
            glob_str = arch.MRT_DIR + arch.UPD_GLOB
            glob_files = glob.glob(glob_str)
            logging.debug(f"Adding {len(glob_files)} from archive {arch.NAME}")
            filelist.extend(glob_files)

    if not filelist:
        logging.info(f"No files found to process")
        return

    parse_files(filelist=filelist, args=args)


def process_range(args: dict) -> None:
    """
    Build a list of MRT files between the --start and --end dates inclusive
    to pass to the MRT parser function.
    """
    if not args:
        raise ValueError(f"Missing required arguments: args={args}")

    if not args["start"] and not args["end"]:
        raise ValueError(
            "Both --start and --end must be specified when using --range"
        )

    start_time = datetime.datetime.strptime(args["start"], cfg.TIME_FORMAT)
    start_day = datetime.datetime(*start_time.timetuple()[0:3])
    end_time = datetime.datetime.strptime(args["end"], cfg.TIME_FORMAT)
    end_day = datetime.datetime(*end_time.timetuple()[0:3])

    if end_time < start_time:
        raise ValueError(
            f"End date {end_time} is before start date {start_time}"
        )

    diff = end_day - start_day
    mrt_a = mrt_archives()
    filelist = []

    for i in range(0, diff.days + 1):
        delta = datetime.timedelta(days=i)
        ymd = datetime.datetime.strftime(start_time + delta, cfg.DAY_FORMAT)

        for arch in mrt_a.archives:
            if args["enabled"] and not arch.ENABLED:
                continue
            logging.debug(f"Checking archive {arch.NAME} on {ymd}...")

            if args["rib"]:
                rib_filenames = arch.gen_rib_fns_day(ymd)

                for filename in rib_filenames[:]:
                    timestamp = arch.ts_from_filename(filename)
                    if timestamp < start_time or timestamp > end_time:
                        rib_filenames.remove(filename)

                if not rib_filenames:
                    continue

                logging.info(
                    f"Adding {len(rib_filenames)} RIB dumps for archive "
                    f"{arch.NAME} on {ymd}"
                )
                logging.debug(f"Adding {rib_filenames}")
                for file in rib_filenames:
                    filelist.append(
                        os.path.normpath(arch.MRT_DIR + "/" + file)
                    )

            if args["update"]:
                upd_filenames = arch.gen_upd_fns_day(ymd)

                for filename in upd_filenames[:]:
                    timestamp = arch.ts_from_filename(filename)
                    if timestamp < start_time or timestamp > end_time:
                        upd_filenames.remove(filename)

                if not upd_filenames:
                    continue

                logging.info(
                    f"Adding {len(upd_filenames)} UPDATE dumps for archive "
                    f"{arch.NAME} on {ymd}"
                )
                logging.debug(f"Adding {upd_filenames}")
                for file in upd_filenames:
                    filelist.append(
                        os.path.normpath(arch.MRT_DIR + "/" + file)
                    )

    if not filelist:
        logging.info(f"No files found to process")
        return

    parse_files(filelist=filelist, args=args)


def main():
    args = parse_args()
    log.setup(
        debug=args["debug"],
        log_src="MRT parser script",
        log_path=cfg.LOG_PARSER,
    )

    args["multi"] = not args["no_multi"]

    if args["continuous"]:
        continuous(args)
    elif args["single"]:
        process_mrt_file(args)
    elif args["yesterday"]:
        delta = datetime.timedelta(days=1)
        yesterday = datetime.datetime.strftime(
            datetime.datetime.now() - delta, cfg.DAY_FORMAT
        )
        args["ymd"] = yesterday
        process_day(args)
    elif args["ymd"]:
        process_day(args)
    elif args["range"]:
        process_range(args)
    else:
        process_mrt_glob(args)


if __name__ == "__main__":
    main()
