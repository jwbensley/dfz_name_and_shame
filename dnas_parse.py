"""
Requires:
pypy3.8-v7.3.7

pypy3 -mpip ensurepip
pypy3 -mpip install --upgrade pip
pypy3 -mpip install mrtparse
pypy3 -mpip install requests
pypy3 -mpip install redis
"""

###import os

import multiprocessing
from multiprocessing import Pool

import sys
sys.path.append('./')
from mrt_data import mrt_data
from mrt_getter import mrt_getter
from mrt_parser import mrt_parser
from mrt_splitter import mrt_splitter


def main():

    # Download the RIB dump and MRT updates from 2 hours ago.
    #files = mrt_getter.get_latest_rv()
    files = ['/tmp/ribv6.20211222.0600.bz2', '/tmp/updates.20220103.1200.bz2', '/tmp/updates.20220103.1215.bz2', '/tmp/updates.20220103.1230.bz2', '/tmp/updates.20220103.1245.bz2', '/tmp/updates.20220103.1300.bz2', '/tmp/updates.20220103.1315.bz2', '/tmp/updates.20220103.1330.bz2', '/tmp/updates.20220103.1345.bz2']
    num_procs =  multiprocessing.cpu_count()
    Pool = multiprocessing.Pool(num_procs)

    for file in files:

        splitter = mrt_splitter(file)
        no_entries, file_chunks = splitter.split(num_procs)
        rib_chunks = Pool.map(mrt_parser.parse_rib_dump, file_chunks)

        rib_data = mrt_data()
        for chunk in rib_chunks:
            rib_data.merge_chunk(chunk)

        mrt_parser.to_file(file + ".json", rib_data)

        break


    #entries = mrtparse.Reader(filename)
    #timestamp = next(entries).data["timestamp"][0]

    #results = merge_chunks(mrt_stats_chunks)
    # ^ Add to results the filename these came from

if __name__ == '__main__':
    main()
