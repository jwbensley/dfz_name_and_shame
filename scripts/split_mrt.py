#!/usr/bin/env python3

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

def main():

    if len(sys.argv) != 3:
        print(f"Wrong number or arguments supplied ({len(sys.argv)}), should be 3")
        print(f"{sys.argv[0]} /path/to/mrt/file <no_of_pieces_to_split_into>")
        exit(1)

    filename = sys.argv[1]
    no_of_chunks = int(sys.argv[2])

    splitter = mrt_splitter(filename)
    total, chunk_names = splitter.split(no_of_chunks)
    print(f"Split {total} mrt_entries into {no_of_chunks} files:")
    print(chunk_names)

if __name__ == '__main__':
    main()