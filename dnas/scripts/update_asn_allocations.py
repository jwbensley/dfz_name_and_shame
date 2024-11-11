#!/usr/bin/env python3

import argparse
import csv
import logging
import os
import sys

# Accommodate the use of the dnas library, even when the library isn't installed
sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
)

from dnas.config import config as cfg
from dnas.log import log
from dnas.mrt_getter import mrt_getter


def download_asn_stats() -> None:
    """
    Download the latest version of the ASN allocation stats file.
    """
    logging.info(
        f"Download ASN stats file to {cfg.asn_stats_file} from "
        f"{cfg.asn_allocation_url}"
    )
    result = mrt_getter.download_file(
        filename=cfg.asn_stats_file, url=cfg.asn_allocation_url, replace=True
    )
    if result == False:
        logging.info(f"No change to existing ASN stats file")
    else:
        logging.info(f"ASN stats file updated")


def parse_args() -> dict:
    """
    Parse the CLI args to this script.
    """
    parser = argparse.ArgumentParser(
        description="Download and parse the latest ASN allocation stats",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--debug",
        help="Enable debug logging for this script.",
        default=False,
        action="store_true",
        required=False,
    )

    return vars(parser.parse_args())


def parse_asn_stats(filename: str) -> list[tuple[int, int]]:
    """
    Parse the ASN stats file and return the ranges of unallocated ASNs.
    """

    unallocated_asn_tuples = []

    with open(filename, "r") as csv_file:
        csv_data = csv.DictReader(csv_file)
        for row in csv_data:
            if row["Description"].lower() == "unallocated":
                start_asn, end_asn = map(int, row["Number"].split("-"))
                unallocated_asn_tuples.append((start_asn, end_asn))

    logging.debug(
        f"Found {len(unallocated_asn_tuples)} unallocated ASN ranges"
    )
    return unallocated_asn_tuples


def update_asn_stats() -> None:
    """
    Update the list of allocated ASNs
    """

    if not os.path.exists(cfg.asn_stats_file):
        logging.warning(
            f"ASN stats file doesn't exist, going to download it..."
        )
        download_asn_stats()

    asns = parse_asn_stats(cfg.asn_stats_file)

    try:
        os.unlink(cfg.unallocated_asns_file)
    except FileNotFoundError:
        pass

    with open(cfg.unallocated_asns_file, "w") as asns_file:
        for asn in asns:
            asns_file.write(f"{asn}\n")

    logging.info(f"Wrote unallocated ASNs to {cfg.unallocated_asns_file}")


def main():
    args = parse_args()
    log.setup(
        debug=args["debug"],
        log_src="ASN allocation script",
        log_path=cfg.LOG_UPDATE_ASN,
    )
    update_asn_stats()


if __name__ == "__main__":
    main()
