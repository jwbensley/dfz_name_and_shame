#!/usr/bin/env python3

import argparse
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
from dnas.mrt_stats import mrt_stats

rdb = redis_db()

def dump_json(filename):
    """
    Dump the entire redis DB to a JSON file.
    """
    rdb.to_file(filename)

def load_json(filename):
    """
    Import a JOSN dump into redis.
    """
    rdb.from_file(filename)

def print_stats(key):
    """
    Print an mrt stats object stored in redis, based on the passed key.
    """
    if not key:
        raise ValueError(
            f"Missing required arguments: key={key}"
        )

    mrt_s = rdb.get_stats(key)
    if mrt_s:
        mrt_s.print()
    else:
        print(f"No stats stored in redis under key {key}")

def print_stats_daily(ymd):
    """
    Print the mrt stats object from a specific day stored in redis.
    """
    if not ymd:
        raise ValueError(
            f"Missing required arguments: ymd={ymd}"
        )

    mrt_s = rdb.get_stats(mrt_stats.gen_daily_key(ymd))
    if mrt_s:
        mrt_s.print()
    else:
        print(f"No stats stored in redis for day {ymd}")

def print_stats_diff(keys):
    """
    Print the diff of two mrt stats objects stored in redis at the two
    passed keys.
    """
    if (not keys):
        raise ValueError(
            f"Missing required arguments: keys={keys}"
        )

    if len(keys) != 2:
        raise ValueError(
            f"Exactly two keys must be provided: keys={keys}"
        )

    mrt_s_1 = rdb.get_stats(keys[0])
    mrt_s_2 = rdb.get_stats(keys[1])
    if not mrt_s_1:
        print(f"Not stats stored in redis under {keys[0]}")
        return
    if not mrt_s_2:
        print(f"Not stats stored in redis under {keys[1]}")
        return

    diff = mrt_s_1.get_diff(mrt_s_2)
    if not diff.is_empty():
        diff.print()
    else:
        print(f"Stats objects are equal")

def print_stats_global():
    """
    Print the global stats object stored in redis.
    """
    mrt_s = rdb.get_stats(mrt_stats.gen_global_key())
    if mrt_s:
        mrt_s.print()
    else:
        print(f"No global stats stored in redis")

def wipe():
    """
    Wipe the entire redis DB.
    """
    for k in rdb.get_keys("*"):
        rdb.delete(k)

def parse_args():
    """
    Parse the CLI args to this script.
    """

    parser = argparse.ArgumentParser(
        description="Managed the redis DB for DNAS",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--dump",
        help="Specify an output filename to dump the entire redis DB to JSON.",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument(
        "--daily",
        help="Specify a specific day and print the day stats stored in redis "
        "for that day. Must use yyyymmdd format e.g., 20221231.",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument(
        "--diff",
        help="Specify two stats keys, and print the difference between the "
        "stats objects stored in redis at those two keys. E.g., --diff "
        "20220101 20220102.",
        type=str,
        nargs=2,
        metavar=("key1", "key2"),
        required=False,
        default=None,
    )
    parser.add_argument(
        "--global",
        help="Print the current global stats stored in redis.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--load",
        help="Specify an intput JSON filename to load in redis. "
        "Any existing keys that match will be overwritten.",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument(
        "--stats",
        help="Print the mrt stats object stored under the specified key.",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument(
        "--wipe",
        help="Erase the entire redis DB",
        default=False,
        action="store_true",
        required=False,
    )

    return vars(parser.parse_args())

def main():

    args = parse_args()

    if args["dump"]:
        dump_json(args["dump"])
    elif args["load"]:
        load_json(args["load"])
    elif args["wipe"]:
        wipe()

    if args["daily"]:
        print_stats_daily(args["daily"])

    if args["diff"]:
        print_stats_diff(args["diff"])

    if args["global"]:
        print_stats_global()

    if args["stats"]:
        print_stats(args["stats"])

    rdb.close()

if __name__ == '__main__':
    main()