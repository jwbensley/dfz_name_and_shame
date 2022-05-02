#!/usr/bin/env python3

import argparse
import errno
import json
import logging
import mrtparse
import os
import sys

# Accomodate the use of the dnas library, even when the library isn't installed
sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)

from dnas.config import config as cfg
from dnas.log import log
from dnas.mrt_parser import mrt_parser
from dnas.mrt_stats import mrt_stats

def check_rib_dump(filename: str = None):
    """
    Perform some basic checks to determin if this is a valid MRT RIB dump.
    """
    if not filename:
        raise ValueError("MRT filename is missing")

    if not os.path.isfile(filename):
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), filename
        )

    mrt_entries = mrtparse.Reader(filename)
    for idx, mrt_e in enumerate(mrt_entries):
        #rib_type = mrt_e.data["type"][0]
        rib_type = list(mrt_e.data["type"].keys())[0]
        if (rib_type != mrtparse.MRT_T['TABLE_DUMP_V2']):
            logging.error(
                f"Entry {idx} in {filename} is not type TABLE_DUMP_V2: "
                f"{mrt_e.data['type']}"
            )
            logging.error(mrt_e.data)
            return

        # RIB dumps can contain both AFIs (v4 and v6)
        #rib_subtype = mrt_e.data["subtype"][0]
        rib_subtype = list(mrt_e.data["subtype"].keys())[0]
        if rib_subtype not in mrtparse.TD_V2_ST:
            logging.error(
                f"Entry {idx} in {filename} is not type PEER_INDEX_TABLE or "
                f"RIB_IPV4_UNICAST or RIB_IPV6_UNICAST: {mrt_e.data['subtype']}"
            )
            logging.error(mrt_e.data)
            return

    logging.info(f"{filename} appears to be a valid RIB dump MRT file.")

def check_update_dump(filename: str = None):
    """
    Perform some basic checks to determin if this is a valid MRT UPDATE
    dump.
    """
    if not filename:
        raise ValueError("MRT filename is missing")

    if not os.path.isfile(filename):
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), filename
        )

    mrt_entries = mrtparse.Reader(filename)
    for idx, mrt_e in enumerate(mrt_entries):
        #upd_type = mrt_e.data["type"][0]
        upd_type = list(mrt_e.data["type"].keys())[0]
        if (upd_type != mrtparse.MRT_T['BGP4MP_ET'] and
            upd_type != mrtparse.MRT_T['BGP4MP']):
            logging.error(
                f"Entry {idx} in {filename} is not type BGP4MP_ET: "
                f"{mrt_e.data['type']}"
            )
            logging.error(mrt_e.data)
            return

        # UPDATE dumps can contain both AFIs (v4 and v6)
        #upd_subtype = mrt_e.data["subtype"][0]
        upd_subtype = list(mrt_e.data["subtype"].keys())[0]
        if upd_subtype not in mrtparse.BGP4MP_ST:
            logging.error(
                f"Entry {idx} in {filename} is not type BGP4MP_MESSAGE or "
                f"BGP4MP_MESSAGE_AS4: {mrt_e.data['subtype']}"
            )
            logging.error(mrt_e.data)
            return

    logging.info(f"{filename} appears to be a valid UPDATE dump MRT file.")

def get_count(filename: str = None):
    """
    Print the number of entries in an MRT file.
    """
    if not filename:
        raise ValueError("MRT filename is missing")

    if not os.path.isfile(filename):
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), filename
        )

    logging.info(f"File {filename} as {mrt_parser.mrt_count(filename)} entries")

def parse_args():
    """
    Parse the CLI args to this script.
    """
    parser = argparse.ArgumentParser(
        description="Perform various test functions against an MRT file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--check-rib",
        help="Check if the file specified with --mrt is a valid RIB dump.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--check-update",
        help="Check if the file specified with --mrt is a valid UPDATE dump.",
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
        "--get-count",
        help="Print the number of entries in the file specified with --mrt.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--mrt",
        help="Path to a single MRT file to test.",
        type=str,
        default=None,
        required=True,
    )
    parser.add_argument(
        "--to-json",
        help="Convert an MRT file to JSON. Specify the path and filename for "
        "the JSON output file.",
        type=str,
        default=None,
        required=False,
    )
    parser.add_argument(
        "--to-json-rib",
        help="Parse an MRT RIB dump file and write the parsed stats to a JSON "
        "file. Specify the full path and filename for the JSON output file.",
        type=str,
        default=None,
        required=False,
    )
    parser.add_argument(
        "--to-json-update",
        help="Parse an MRT UPDATE file and write the parsed stats to a JSON "
        "file. Specify the full path and filename for the JSON output file.",
        type=str,
        default=None,
        required=False,
    )

    return vars(parser.parse_args())

def to_json(json_file: str = None, mrt_file: str = None):
    """
    Convert an MRT file to a JSON string and write to a file.
    """
    if not mrt_file or not json_file:
        raise ValueError(
            f"Missing required arguments: mrt_file={mrt_file}, "
            f"json_file={json_file}"
        )

    mrt_data = []
    for mrt_entry in mrtparse.Reader(mrt_file):
        mrt_data.append(mrt_entry.data)

    with open(json_file, "w") as f:
        f.write(json.dumps(mrt_data, indent=2))
    logging.info(f"Wrote JSON dump to {json_file}")

def to_json_parsed(rib: bool = False, json_file: str = None, mrt_file: str = None):
    """
    Parse an MRT file to generate stats, then write the stats as a JSON string
    to a file.
    """
    if not mrt_file or not json_file:
        raise ValueError(
            f"Missing required arguments: mrt_file={mrt_file}, "
            f"json_file={json_file}"
        )

    ######## TODO - Fix this hack so that mrt_parser doesn't car about the path
    cfg.SPLIT_DIR = ""

    if rib:
        stats: 'mrt_stats' = mrt_parser.parse_rib_dump(mrt_file)
    else:
        stats: 'mrt_stats' = mrt_parser.parse_upd_dump(mrt_file)

    if stats:
        stats.to_file(json_file)
        logging.info(f"Wrote parsed JSON to {json_file}")

def main():

    args = parse_args()
    log.setup(
        debug = args["debug"],
        log_src = "MRT tester script",
        log_path = cfg.LOG_TESTER,
    )

    if not os.path.isfile(args["mrt"]):
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), args["mrt"]
        )

    if args["check_rib"]:
        check_rib_dump(args["mrt"])

    if args["check_update"]:
        check_update_dump(args["mrt"])

    if args["get_count"]:
        get_count(args["mrt"])

    if args["to_json"]:
        to_json(args["to_json"], args["mrt"])

    if args["to_json_rib"]:
        to_json_parsed(
            rib = True,
            json_file = args["to_json_rib"],
            mrt_file = args["mrt"],
        )

    if args["to_json_update"]:
        to_json_parsed(
            rib = False,
            json_file = args["to_json_update"],
            mrt_file = args["mrt"],
        )

if __name__ == '__main__':
    main()
