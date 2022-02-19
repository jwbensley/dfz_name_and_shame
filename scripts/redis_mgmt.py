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

rdb = redis_db()

def dump_json(filename):
    """
    Dump the entire redis DB to a JSON file.
    """
    rdb.to_file(filename)


def get_stats():
def get_stats_diff():
def get_stats_daily():
def get_stats_global():


def load_json(filename):
    """
    Import a JOSN dump into redis.
    """
    rdb.from_file(filename)

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
        "--load",
        help="Specify an intput JSON filename to load in redis. "
        "Any existing keys that match will be overwritten.",
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

    rdb.close()

if __name__ == '__main__':
    main()