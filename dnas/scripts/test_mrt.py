#!/usr/bin/env python3

import argparse
from enum import Enum
import errno
import json
import logging
import os
import sys
import typing

import mrtparse  # type: ignore

# Accomodate the use of the dnas library, even when the library isn't installed
sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
)

from dnas.config import config as cfg
from dnas.log import log
from dnas.mrt_parser import mrt_parser
from dnas.mrt_stats import mrt_stats

class MrtType(str, Enum):
    RIB = "rib"
    UPDATE = "udpate"
    UNKNOWN = "unknown"

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
        rib_type = list(mrt_e.data["type"].keys())[0]
        if rib_type != mrtparse.MRT_T["TABLE_DUMP_V2"]:
            logging.error(
                f"Entry {idx} in {filename} is not type TABLE_DUMP_V2: "
                f"{mrt_e.data['type']}"
            )
            logging.error(mrt_e.data)
            return

        # RIB dumps can contain both AFIs (v4 and v6)
        rib_subtype = list(mrt_e.data["subtype"].keys())[0]
        if rib_subtype not in mrtparse.TD_V2_ST:
            logging.error(
                f"Entry {idx} in {filename} is not type PEER_INDEX_TABLE or "
                f"RIB_IPV4_UNICAST or RIB_IPV6_UNICAST: {mrt_e.data['subtype']}"
            )
            logging.error(mrt_e.data)
            return

    logging.info(f"{filename} appears to be a valid RIB dump MRT file.")


def check_update_dump(filename: str) -> None:
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
        upd_type = list(mrt_e.data["type"].keys())[0]
        if (
            upd_type != mrtparse.MRT_T["BGP4MP_ET"]
            and upd_type != mrtparse.MRT_T["BGP4MP"]
        ):
            logging.error(
                f"Entry {idx} in {filename} is not type BGP4MP_ET: "
                f"{mrt_e.data['type']}"
            )
            logging.error(mrt_e.data)
            return

        # UPDATE dumps can contain both AFIs (v4 and v6)
        upd_subtype = list(mrt_e.data["subtype"].keys())[0]
        if upd_subtype not in mrtparse.BGP4MP_ST:
            logging.error(
                f"Entry {idx} in {filename} is not type BGP4MP_MESSAGE or "
                f"BGP4MP_MESSAGE_AS4: {mrt_e.data['subtype']}"
            )
            logging.error(mrt_e.data)
            return

    logging.info(f"{filename} appears to be a valid UPDATE dump MRT file.")


def get_stats(filename: str) -> None:
    """
    Print some basic statis about the entries in an MRT file.
    """
    if not filename:
        raise ValueError("MRT filename is missing")

    if not os.path.isfile(filename):
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), filename
        )

    attrs = {}
    e_types = {}
    e_subtypes = {}
    e_msgtypes = {}
    as_paths = []
    origin_asns = set()
    prefixes = set()
    peers = {}

    mrt_entries = mrtparse.Reader(filename)
    """
    Check if RIB dump or UPDATE dump
    """

    mrt_type: str
    for mrt_e in mrt_entries:
        if list(mrt_e.data["type"].keys())[0] in [12, 13]:
            mrt_type = MrtType.RIB
            logging.info("Assuming MRT is a RIB dump")

            # Build the peer index table
            mrt_e.data["collector_bgp_id"]
            mrt_e.data["view_name"]
            for idx, peer in enumerate(mrt_e.data["peer_entries"]):
                peers[idx] = {
                    "peer_ip": peer["peer_ip"],
                    "peer_as": peer["peer_as"],
                    "v4_count": 0,
                    "v6_count": 0,
                }

        elif list(mrt_e.data["type"].keys())[0] in [16, 17]:
            mrt_type = MrtType.UPDATE
            logging.info("Assuming MRT is an UPDATE dump")
        else:
            mrt_type = MrtType.UNKNOWN
            logging.warning(f"Unknown MRT type: {mrt_e.data['type']}")
        break

    for idx, mrt_e in enumerate(mrt_entries):
        e_type = list(mrt_e.data["type"].keys())[0]
        if e_type not in e_types:
            e_types[e_type] = 1
        else:
            e_types[e_type] += 1

        e_subtype = list(mrt_e.data["subtype"].keys())[0]
        if e_subtype not in e_subtypes:
            e_subtypes[e_subtype] = 1
        else:
            e_subtypes[e_subtype] += 1

        if mrt_type == MrtType.RIB:
            if list(mrt_e.data["subtype"].keys())[0] == 2: # RIB_IPV4_UNICAST
                key = "v4_count"
            elif list(mrt_e.data["subtype"].keys())[0] == 4: # RIB_IPV6_UNICAST
                key = "v6_count"
            else:
                logging.warning(f"Unknown MRT entry subtype: {mrt_e.data['subtype']}")
                continue
            for entry in mrt_e.data["rib_entries"]:
                peers[entry["peer_index"]][key] += 1


        if mrt_type == MrtType.UPDATE:
            """
            Some UPDATE MRTs contain the BGP state change events
            """
            if e_subtype not in [1, 4]:
                continue

            """
            Some UPDATE MRT files contain empty updates
            """
            if "bgp_message" not in mrt_e.data:
                continue

            """
            Some MRTs contain the BGP messages types (OPEN, KEEPALIVE, etc)
            """
            e_msgtype = next(iter(mrt_e.data["bgp_message"]["type"]))
            if e_msgtype not in e_msgtypes:
                e_msgtypes[e_msgtype] = 1
            else:
                e_msgtypes[e_msgtype] += 1

            if e_msgtype != 2: # UPDATE
                continue

            if len(mrt_e.data["bgp_message"]["nlri"]) > 0:
                for nlri in mrt_e.data["bgp_message"]["nlri"]:
                    prefixes.add(
                        nlri["prefix"] + "/" + str(nlri["length"])
                    )

            if withdrawn_routes := mrt_e.data["bgp_message"].get("withdrawn_routes"):
                for withdrawn_route in withdrawn_routes:
                    prefixes.add(
                        withdrawn_route["prefix"] + "/"
                        + str(withdrawn_route["length"])
                    )

            if path_attributes := mrt_e.data["bgp_message"].get("path_attributes"):
                for attr in path_attributes:
                    attr_t = next(iter(attr["type"]))

                    if attr_t not in attrs:
                        attrs[attr_t] = 1
                    else:
                        attrs[attr_t] += 1

                    if attr_t == 2: # AS_PATH
                        if attr["value"][0]["value"] not in as_paths:
                            as_paths.append(attr["value"][0]["value"])
                        origin_asns.add(attr["value"][0]["value"][-1])

                    if attr_t == 14: # MP_REACH_NLRI -> IPV6_UNICAST
                        for nlri in attr["value"]["nlri"]:
                            prefixes.add(
                                nlri["prefix"] + "/" + str(nlri["length"])
                            )

                    elif attr_t == 15: # MP_UNREACH_NLRI -> IPV6_UNICAST
                        if withdrawn_routes := attr["value"].get("withdrawn_routes"):
                            for withdrawn_route in withdrawn_routes:
                                prefixes.add(
                                    withdrawn_route["prefix"] + "/"
                                    + str(withdrawn_route["length"])
                                )

    logging.info(f"File {filename} contains {idx+1} entries")

    logging.info("Count per record type:")
    for k, v in e_types.items():
        logging.info(f"{mrtparse.MRT_T[k]}: {v}")

    logging.info("Count per record sub-type:")
    for k, v in e_subtypes.items():
        logging.info(f"{mrtparse.BGP4MP_ST[k]}: {v}")

    logging.info("Count per BGP message type:")
    for k, v in e_msgtypes.items():
        logging.info(f"{mrtparse.BGP_MSG_T[k]}: {v}")

    logging.info(f"Count per BGP attribute type:")
    for k, v in attrs.items():
        logging.info(f"Attr {mrtparse.BGP_ATTR_T[k]}: {v}")

    logging.info(f"Unique AS paths: {len(as_paths)}")
    logging.info(f"Unique prefixes: {len(prefixes)}")
    logging.info(f"Unique origin ASNs: {len(origin_asns)}")

    if mrt_type == MrtType.RIB:
        # Sort peers by AS number:
        sorted_peers = []
        for peer in peers.values():
            for idx, sorted_peer in enumerate(sorted_peers):
                if peer["peer_as"] <= sorted_peer["peer_as"]:
                    sorted_peers.insert(idx, peer)
                    break
            else:
                sorted_peers.append(peer)
        sorted_peers.reverse()

        logging.info("Peer list:")
        logging.info(f"| {'Peer AS': <12} | {'Peer IP': <40} | {'v4 Count': <8} | {'v6 Count': <8} |")
        for peer in sorted_peers:
            logging.info(f"| {peer['peer_as']: <12} | {peer['peer_ip']: <40} | {peer['v4_count']: <8} | {peer['v6_count']: <8} |")



def parse_args() -> dict:
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
        "--get-stats",
        help="Print some basic stats about the entries in the file specified "
        "with --mrt.",
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


def to_json(json_file: str, mrt_file: str) -> None:
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


def to_json_parsed(json_file: str, mrt_file: str, rib: bool = False) -> None:
    """
    Parse an MRT file to generate stats, then write the stats as a JSON string
    to a file.
    """
    if not mrt_file or not json_file:
        raise ValueError(
            f"Missing required arguments: mrt_file={mrt_file}, "
            f"json_file={json_file}"
        )

    ######## TODO - Fix this hack so that mrt_parser doesn't care about the path
    cfg.SPLIT_DIR = ""

    stats: "mrt_stats"
    if rib:
        stats = mrt_parser.parse_rib_dump(mrt_file)
    else:
        stats = mrt_parser.parse_upd_dump(mrt_file)

    if stats:
        stats.to_file(json_file)
        logging.info(f"Wrote parsed JSON to {json_file}")


def main():
    args = parse_args()
    log.setup(
        debug=args["debug"],
        log_src="MRT tester script",
        log_path=cfg.LOG_TESTER,
    )

    if not os.path.isfile(args["mrt"]):
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), args["mrt"]
        )

    args["mrt"] = os.path.abspath(args["mrt"])

    if args["check_rib"]:
        check_rib_dump(args["mrt"])

    if args["check_update"]:
        check_update_dump(args["mrt"])

    if args["get_stats"]:
        get_stats(args["mrt"])

    if args["to_json"]:
        to_json(args["to_json"], args["mrt"])

    if args["to_json_rib"]:
        to_json_parsed(
            rib=True,
            json_file=args["to_json_rib"],
            mrt_file=args["mrt"],
        )

    if args["to_json_update"]:
        to_json_parsed(
            rib=False,
            json_file=args["to_json_update"],
            mrt_file=args["mrt"],
        )


if __name__ == "__main__":
    main()
