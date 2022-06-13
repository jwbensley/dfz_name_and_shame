#!/usr/bin/env python3

import argparse
import logging
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
from dnas.mrt_splitter import mrt_splitter

def parse_args():
    """
    Parse the CLI args to this script.
    """
    parser = argparse.ArgumentParser(
        description="Split an MRT file into N equal sized chunks. "
        "The chunks will be written to the same directory as the input file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--chunks",
        help="Number fo chunks to split the file into.",
        type=int,
        default=None,
        required=True,
    )
    parser.add_argument(
        "--debug",
        help="Run with debug level logging.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--filename",
        help="Full path to MRT file to split.",
        type=str,
        default=None,
        required=True,
    )
    return vars(parser.parse_args())

def split(filename: str = None, num_chunks: int = None):
    """
    Split an MRT file into N equal sized files ("chunks").
    """
    if not filename or not num_chunks:
        raise ValueError(
            f"Missing required arguments: filename={filename}, "
            f"num_chunks={num_chunks}"
        )

    splitter = mrt_splitter(filename)
    try:
        num_entires, chunk_names = splitter.split(num_chunks)
    except EOFError as e:
        logging.error(f"Unable to split {filename}, unexpeted EOF")
        raise
    logging.info(f"Split {num_entires} MRT entries into {len(chunk_names)} files:")
    logging.info(chunk_names)

def main():

    args = parse_args()
    log.setup(
        debug = args["debug"],
        log_src = "MRT splitter script",
        log_path = cfg.LOG_SPLITTER,
    )

    split(args["filename"], int(args["chunks"]))


if __name__ == '__main__':
    main()