"""
Requires:
pypy3.8-v7.3.7

pypy3 -mpip ensurepip
pypy3 -mpip install --upgrade pip
pypy3 -mpip install mrtparse
pypy3 -mpip install requests
pypy3 -mpip install redis
"""

import mrtparse
import os


import timeit
import itertools
import operator

import math
import multiprocessing
from multiprocessing import Pool

import sys
sys.path.append('./')
from mrt_getter import mrt_getter
from mrt_parser import mrt_parser


def main():

    # Download the RIB dump and MRT updates from 2 hours ago.
    #files = mrt_getter.get_latest_rv()
    files = ['/tmp/rib.20220103.1200.bz2', '/tmp/updates.20220103.1200.bz2', '/tmp/updates.20220103.1215.bz2', '/tmp/updates.20220103.1230.bz2', '/tmp/updates.20220103.1245.bz2', '/tmp/updates.20220103.1300.bz2', '/tmp/updates.20220103.1315.bz2', '/tmp/updates.20220103.1330.bz2', '/tmp/updates.20220103.1345.bz2']
    num_procs =  multiprocessing.cpu_count()
    Pool = multiprocessing.Pool(num_procs)

    for file in files:

        splitter = mrt_splitter(file)
        no_entries, file_chunks = splitter.split(num_procs)
        print(file_chunks)
        stats_chunks = Pool.map(load_parse_mrt_update, file_chunks)

        break


    entries = mrtparse.Reader(filename)
    timestamp = next(entries).data["timestamp"][0]



    results = merge_chunks(mrt_stats_chunks)
    # ^ Add to results the filename these came from

if __name__ == '__main__':
    main()
