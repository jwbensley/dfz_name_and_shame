#!/usr/bin/env python3

import argparse
import json
import logging
import os
import sys
from typing import Any

# Accommodate the use of the dnas library, even when the library isn't installed
sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
)

from dnas.config import config as cfg
from dnas.log import log
from dnas.mrt_entry import mrt_entry
from dnas.mrt_stats import mrt_stats
from dnas.redis_db import redis_db

rdb = redis_db()


def get_year_stats(args: argparse.Namespace) -> dict[Any, Any]:
    """
    Get stats for a year of DNAS data.
    """
    logging.info(f"Getting stats for {args.year}")

    longest_as_path: list[mrt_entry] = []
    longest_comm_set: list[mrt_entry] = []
    most_advt_prefixes: list[mrt_entry] = []
    most_upd_prefixes: list[mrt_entry] = []
    most_withd_prefixes: list[mrt_entry] = []
    most_advt_origin_asn: list[mrt_entry] = []
    most_advt_peer_asn: list[mrt_entry] = []
    most_upd_peer_asn: list[mrt_entry] = []
    most_withd_peer_asn: list[mrt_entry] = []
    most_origin_asns: list[mrt_entry] = []
    most_unknown_attrs: list[mrt_entry] = []
    most_unreg_origins: list[mrt_entry] = []

    longest_as_path_per_day: dict[int, int] = {}
    longest_comm_set_per_day: dict[int, int] = {}
    most_advt_prefixes_per_day: dict[int, int] = {}
    most_upd_prefixes_per_day: dict[int, int] = {}
    most_withd_prefixes_per_day: dict[int, int] = {}
    most_advt_origin_asn_per_day: dict[int, int] = {}
    most_advt_peer_asn_per_day: dict[int, int] = {}
    most_upd_peer_asn_per_day: dict[int, int] = {}
    most_withd_peer_asn_per_day: dict[int, int] = {}
    most_origin_asns_per_day: dict[int, int] = {}
    most_unknown_attrs_per_day: dict[int, int] = {}
    most_unreg_origins_per_day: dict[int, int] = {}

    doy = 0
    for month in range(1, 13):
        for day in range(1, 32):

            if month == 2 and day > 28:
                continue
            if month in [4, 6, 9, 11] and day > 30:
                continue
            ymd = f"{args.year}{month:02d}{day:02d}"
            doy += 1

            logging.debug(f"Getting stats for {ymd}")

            mrt_s = rdb.get_stats(
                key=mrt_stats.gen_daily_key(ymd), compression=args.compression
            )

            if not mrt_s:
                continue
                # raise ValueError(f"Missing stats for {ymd}")

            longest_as_path_per_day[doy] = len(
                mrt_s.longest_as_path[0].as_path
            )

            if not longest_as_path or len(
                mrt_s.longest_as_path[0].as_path
            ) > len(longest_as_path[0].as_path):
                longest_as_path = mrt_s.longest_as_path

            longest_comm_set_per_day[doy] = len(
                mrt_s.longest_comm_set[0].comm_set
            )

            if not longest_comm_set or len(
                mrt_s.longest_comm_set[0].comm_set
            ) > len(longest_comm_set[0].comm_set):
                longest_comm_set = mrt_s.longest_comm_set

            most_advt_prefixes_per_day[doy] = mrt_s.most_advt_prefixes[0].advt
            if (
                not most_advt_prefixes
                or mrt_s.most_advt_prefixes[0].advt
                > most_advt_prefixes[0].advt
            ):
                most_advt_prefixes = mrt_s.most_advt_prefixes

            most_upd_prefixes_per_day[doy] = mrt_s.most_upd_prefixes[0].updates
            if (
                not most_upd_prefixes
                or mrt_s.most_upd_prefixes[0].updates
                > most_upd_prefixes[0].updates
            ):
                most_upd_prefixes = mrt_s.most_upd_prefixes

            most_withd_prefixes_per_day[doy] = mrt_s.most_withd_prefixes[
                0
            ].withdraws
            if (
                not most_withd_prefixes
                or mrt_s.most_withd_prefixes[0].withdraws
                > most_withd_prefixes[0].withdraws
            ):
                most_withd_prefixes = mrt_s.most_withd_prefixes

            most_advt_origin_asn_per_day[doy] = mrt_s.most_advt_origin_asn[
                0
            ].advt
            if (
                not most_advt_origin_asn
                or mrt_s.most_advt_origin_asn[0].advt
                > most_advt_origin_asn[0].advt
            ):
                most_advt_origin_asn = mrt_s.most_advt_origin_asn

            most_advt_peer_asn_per_day[doy] = mrt_s.most_advt_peer_asn[0].advt
            if (
                not most_advt_peer_asn
                or mrt_s.most_advt_peer_asn[0].advt
                > most_advt_peer_asn[0].advt
            ):
                most_advt_peer_asn = mrt_s.most_advt_peer_asn

            most_upd_peer_asn_per_day[doy] = mrt_s.most_upd_peer_asn[0].updates
            if not most_upd_peer_asn or (
                mrt_s.most_upd_peer_asn[0].updates
                > most_upd_peer_asn[0].updates
            ):
                most_upd_peer_asn = mrt_s.most_upd_peer_asn

            most_withd_peer_asn_per_day[doy] = mrt_s.most_withd_peer_asn[
                0
            ].withdraws
            if not most_withd_peer_asn or (
                mrt_s.most_withd_peer_asn[0].withdraws
                > most_withd_peer_asn[0].withdraws
            ):
                most_withd_peer_asn = mrt_s.most_withd_peer_asn

            most_origin_asns_per_day[doy] = len(
                mrt_s.most_origin_asns[0].origin_asns
            )
            if not most_origin_asns or len(
                mrt_s.most_origin_asns[0].origin_asns
            ) > len(most_origin_asns[0].origin_asns):
                most_origin_asns = mrt_s.most_origin_asns

            # Some newer stats don't exist in REDIS for older data
            if not mrt_s.most_unknown_attrs:
                most_unknown_attrs_per_day[doy] = 0
            else:
                most_unknown_attrs_per_day[doy] = len(
                    mrt_s.most_unknown_attrs[0].unknown_attrs
                )
            if not most_unknown_attrs or len(
                mrt_s.most_unknown_attrs[0].unknown_attrs
            ) > len(most_unknown_attrs[0].unknown_attrs):
                most_unknown_attrs = mrt_s.most_unknown_attrs

            if not mrt_s.most_unreg_origins:
                most_unreg_origins_per_day[doy] = 0
            else:
                most_unreg_origins_per_day[doy] = len(
                    mrt_s.most_unreg_origins[0].origin_asns
                )
            if not most_unreg_origins or len(
                mrt_s.most_unreg_origins[0].origin_asns
            ) > len(most_unreg_origins[0].origin_asns):
                most_unreg_origins = mrt_s.most_unreg_origins

    return {
        "longest_as_path": [e.to_dict() for e in longest_as_path],
        "longest_as_path_per_day": longest_as_path_per_day,
        "longest_comm_set": [e.to_dict() for e in longest_comm_set],
        "longest_comm_set_per_day": longest_comm_set_per_day,
        "most_advt_prefixes": [e.to_dict() for e in most_advt_prefixes],
        "most_advt_prefixes_per_day": most_advt_prefixes_per_day,
        "most_upd_prefixes": [e.to_dict() for e in most_upd_prefixes],
        "most_upd_prefixes_per_day": most_upd_prefixes_per_day,
        "most_withd_prefixes": [e.to_dict() for e in most_withd_prefixes],
        "most_withd_prefixes_per_day": most_withd_prefixes_per_day,
        "most_advt_origin_asn": [e.to_dict() for e in most_advt_origin_asn],
        "most_advt_origin_asn_per_day": most_advt_origin_asn_per_day,
        "most_advt_peer_asn": [e.to_dict() for e in most_advt_peer_asn],
        "most_advt_peer_asn_per_day": most_advt_peer_asn_per_day,
        "most_upd_peer_asn": [e.to_dict() for e in most_upd_peer_asn],
        "most_upd_peer_asn_per_day": most_upd_peer_asn_per_day,
        "most_withd_peer_asn": [e.to_dict() for e in most_withd_peer_asn],
        "most_withd_peer_asn_per_day": most_withd_peer_asn_per_day,
        "most_origin_asns": [e.to_dict() for e in most_origin_asns],
        "most_origin_asns_per_day": most_origin_asns_per_day,
        "most_unknown_attrs": [e.to_dict() for e in most_unknown_attrs],
        "most_unknown_attrs_per_day": most_unknown_attrs_per_day,
        "most_unreg_origins": [e.to_dict() for e in most_unreg_origins],
        "most_unreg_origins_per_day": most_unreg_origins_per_day,
    }


def redis_load_json(args: argparse.Namespace) -> None:
    """
    Import a JSON dump into redis.
    """
    if not args.load:
        logging.info(
            f"No input file specified, skipping loading JSON into REDIS"
        )
        return

    if args.stream:
        rdb.from_file_stream(
            compression=args.compression, filename=args.load, daily_only=True
        )
    else:
        rdb.from_file(
            compression=args.compression, filename=args.load, daily_only=True
        )
    logging.info(f"Loaded DB dump from {args.load}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract stats for a year from DNAS DB.",
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
        "--year",
        help="The year to generate stats for.",
        type=str,
        default=None,
        required=True,
    )

    input = parser.add_argument_group("Input Options")
    input.add_argument(
        "--load",
        help="Specify an input JSON filename to load in to redis. "
        "Any existing keys that match will be overwritten.",
        type=str,
        required=False,
        default=None,
    )
    input.add_argument(
        "--no-compression",
        help="Disable compression When dumping/loading a REDIS dump JSON file",
        default=False,
        action="store_true",
        required=False,
    )
    input.add_argument(
        "--stream",
        help="When dumping/loading from a JSON file, stream the data",
        default=False,
        action="store_true",
        required=False,
    )

    output = parser.add_argument_group("Output Options")
    output.add_argument(
        "--output",
        help="The output directory to write the stats to.",
        type=str,
        default=cfg.YEAR_STATS_BASE,
        required=False,
    )

    args = parser.parse_args()
    args.compression = not args.no_compression
    return args


def wipe() -> None:
    """
    Wipe the entire redis DB.
    """

    confirm = input("This will wipe the DB, continue? (y/n) ")
    if confirm.lower() != "y":
        logging.info("Not wiping")
        return

    for k in rdb.get_keys("*"):
        rdb.delete(k)
    logging.info(f"Database wiped")


def write_json(args: argparse.Namespace, data: dict[Any, Any]) -> None:
    if not os.path.exists(args.output):
        os.makedirs(args.output)
        logging.info(f"Created output directory: {args.output}")

    filename = os.path.join(args.output, f"{args.year}.json")
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    logging.info(f"Wrote JSON data to {filename}")


def main() -> None:
    args = parse_args()
    log.setup(
        debug=args.debug,
        log_src="Year stats script",
        log_path=cfg.LOG_YEAR_STATS,
    )

    wipe()
    redis_load_json(args)
    stats = get_year_stats(args)
    write_json(args, stats)
    logging.info("Done!")


if __name__ == "__main__":
    main()
