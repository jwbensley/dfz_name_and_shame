#!/usr/bin/env python3

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

def main():

    args = parse_args()

    if args["debug"]:
        logging.basicConfig(
            format='%(asctime)s %(levelname)s %(funcName)s %(message)s',
            level=logging.DEBUG
        )
    else:
        logging.basicConfig(
            format='%(asctime)s %(levelname)s %(message)s',
            level=logging.INFO
        )

    logging.info(f"Starting MRT splitter with logging level {level}")

    splitter = mrt_splitter(args["filename"])
    total, chunk_names = splitter.split(args["chunks"])
    logging.info(f"Split {total} MRT entries into {len(chunk_names)} files:")
    logging.info(chunk_names)

if __name__ == '__main__':
    main()