#!/usr/bin/env python3

import argparse
import datetime
import logging
import os
import sys

# Accomodate the use of the script, even when the dnas library isn't installed
sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)

from dnas.redis_db import redis_db
from dnas.mrt_archives import mrt_archives
from dnas.mrt_archive import mrt_archive
from dnas.mrt_stats import mrt_stats
from dnas.mrt_entry import mrt_entry

def gen_day_stats(args):
    """
    Generate the global stats for a specific day, by merging the stats obj from
    each MRT archive of that day.
    """
    if not args:
        raise ValueError(
            f"Missing required arguments: args={args}"
        )

    if not args["ymd"]:
        raise ValueError(
            f"Missing required arguments: ymd={args['ymd']}, use --ymd"
        )

    if (not args["rib"] and not args["update"]):
        raise ValueError(
            "At least one of --rib and/or --update must be used with "
            "--daily"
        )

    rdb = redis_db()
    mrt_a = mrt_archives()
    day_stats = mrt_stats()
    day_stats.timestamp = mrt_stats.gen_ts_from_ymd(args["ymd"])

    for arch in mrt_a.archives:
        if (args["enabled"] and not arch.ENABLED):
            continue
        logging.debug(f"Archive {arch.NAME} is enabled")

        if args["rib"]:
            day_key = arch.gen_rib_key(args["ymd"])
            arch_stats = rdb.get_stats(day_key)

            if arch_stats:
                if day_stats.merge(arch_stats):
                    logging.info(
                        f"Compiling {day_key} RIB stats into daily stats for "
                        f"{args['ymd']}"
                    )
                else:
                    logging.info(
                        f"No contribution from {day_key} RIB to daily stats "
                        f"for {args['ymd']}"
                    )

        if args["update"]:
            day_key = arch.gen_upd_key(args["ymd"])
            arch_stats = rdb.get_stats(day_key)

            if arch_stats:
                if day_stats.merge(arch_stats):
                    logging.info(
                        f"Compiling {day_key} UPDATE stats into daily stats "
                        f"for {args['ymd']}"
                    )
                else:
                    logging.info(
                        f"No contribution from {day_key} UPDATE to daily stats "
                        f"for {args['ymd']}"
                    )

    day_key = mrt_stats.gen_daily_key(args["ymd"])
    db_day_stats = rdb.get_stats(day_key)
    if not db_day_stats:
        logging.info(
            f"No existing global stats obj for day {args['ymd']}, "
            "storing compiled stats"
        )
        rdb.set_stats(day_key, day_stats)
    else:
        logging.debug(f"Retrieved existing day stats from {day_key}")
        if db_day_stats.merge(day_stats):
            rdb.set_stats(day_key, db_day_stats)
            logging.info(
                f"Merged {args['ymd']} stats with existing day stats in redis"
            )
        else:
            logging.info(
                f"No update to exsiting {args['ymd']} stats in redis"
            )

    rdb.close()

def gen_diff(ymd):
    """
    Generate and store the diff of a daily stats object, with the daily stats
    from the day before.
    """
    if not ymd:
        raise ValueError(
            f"Missing required arguments: ymd={ymd}, use --ymd"
        )

    rdb = redis_db()

    day_key = mrt_stats.gen_daily_key(ymd)
    day_stats = rdb.get_stats(day_key)
    if not day_stats:
        logging.info(
            f"No existing global stats obj for day {day_key}, "
            "nothing to diff"
        )
        return

    prev_key = mrt_stats.gen_prev_daily_key(ymd)
    prev_sats = rdb.get_stats(prev_key)
    if not prev_sats:
        logging.info(
            f"No existing global stats obj for day {prev_key}, "
            "nothing to diff"
        )
        return

    diff_key = mrt_stats.gen_diff_key(ymd)
    existing_diff = rdb.get_stats(diff_key)
    new_diff = prev_sats.get_diff_larger(day_stats)

    if not existing_diff:
        logging.info(f"No exisiting diff stored under {diff_key}")

        if new_diff.is_empty():
            logging.info(
                f"Storing empty diff stats for {ymd} under {diff_key}"
            )
        else:
            logging.info(f"Storing new diff stats for {ymd} under {diff_key}")
        rdb.set_stats(diff_key, new_diff)

    else:
        if new_diff.is_empty():
            logging.info(
                f"No difference between existing diff and new diff for {ymd}"
            )
        else:
            logging.info(
                f"Overwitten existing diff for {ymd} under {diff_key}"
            )
            rdb.set_stats(diff_key, new_diff)

    rdb.close()
    return

def parse_args():
    """
    Parse the CLI args to this script.
    """

    parser = argparse.ArgumentParser(
        description="Merge individual stats stored in redis, into global stats.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--daily",
        help="Generate and store in redis the daily stats for the day "
        "specified with --ymd.",
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
        "--diff",
        help="Generate and store in redis the diff between the daily stats "
        "for the day specified with --ymd, to those from the day before.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--global",
        help="Update the running global stats stored in redis with the stats "
        "from the day specified using --ymd.",
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
        help="Generate daily stats for parsed RIB MRT dumps.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--update",
        help="Generate daily stats for parsed update MRT dumps.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--ymd",
        help="Date must be in the format yyyymmdd e.g., 20211231.",
        type=str,
        default=None,
        required=False,
    )
    return vars(parser.parse_args())

def upd_global_with_day(args):
    """
    Update the running global stats object with the global stats from a
    specific day.
    """
    if not args:
        raise ValueError(
            f"Missing required arguments: args={args}"
        )
    if not args["ymd"]:
        raise ValueError(
            f"Missing required arguments: ymd={args['ymd']}, use --ymd"
        )

    rdb = redis_db()
    global_key = mrt_stats.gen_global_key()
    global_stats = rdb.get_stats(global_key)

    day_key = mrt_stats.gen_daily_key(args["ymd"])
    day_stats = rdb.get_stats(day_key)

    if not day_stats:
        logging.info(
            f"No existing day stats for {args['ymd']} in redis. Nothing to "
            "update"
        )
        return

    if not global_stats:
        logging.info(
            f"No existing gobal stats in redis, creating new entry with day "
            f"stats for {args['ymd']}"
        )
        rdb.set_stats(global_key, day_stats)
    
    # Else there are global stats and day stats to merge
    else:
        if global_stats.merge(day_stats):
            logging.info(
                f"Global stats merged with day stats from {args['ymd']}"
            )
            rdb.set_stats(global_key, global_stats)
        else:
            logging.info(
                f"No update to global stats with day stats from {args['ymd']}"
            )

    rdb.close()

def main():

    args = parse_args()

    os.makedirs(os.path.dirname(cfg.LOG_DIR), exist_ok=True)
    if args["debug"]:
        logging.basicConfig(
            format='%(asctime)s %(levelname)s %(funcName)s %(message)s',
            level=logging.DEBUG,
            handlers=[
                logging.FileHandler(cfg.LOG_STATS, mode=cfg.LOG_MODE),
                logging.StreamHandler()
            ]
        )
    else:
        logging.basicConfig(
            format='%(asctime)s %(levelname)s %(message)s',
            level=logging.INFO,
            handlers=[
                logging.FileHandler(cfg.LOG_STATS, mode=cfg.LOG_MODE),
                logging.StreamHandler()
            ]
        )

    logging.info(
        f"Starting global stats compiler with logging level "
        f"{logging.getLevelName(logging.getLogger().getEffectiveLevel())}"
    )

    if args["daily"]:
        gen_day_stats(args)

    if args["diff"]:
        gen_diff(args["ymd"])

    if args["global"]:
        upd_global_with_day(args)

if __name__ == '__main__':
    main()