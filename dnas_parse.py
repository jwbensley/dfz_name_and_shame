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
    files = ['http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/RIBS/rib.20220103.1200.bz2', 'http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/UPDATES/updates.20220103.1200.bz2', 'http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/UPDATES/updates.20220103.1215.bz2', 'http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/UPDATES/updates.20220103.1230.bz2', 'http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/UPDATES/updates.20220103.1245.bz2', 'http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/UPDATES/updates.20220103.1300.bz2', 'http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/UPDATES/updates.20220103.1315.bz2', 'http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/UPDATES/updates.20220103.1330.bz2', 'http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/UPDATES/updates.20220103.1345.bz2']

    rib_file = files[0]
    rib_data = mrt_parser(rib_file)

    for file in files:


    num_procs =  multiprocessing.cpu_count()
    Pool = multiprocessing.Pool(num_procs)

    print(f"Starting processes...")


    entries = mrtparse.Reader(filename)
    timestamp = next(entries).data["timestamp"][0]

    """
    args = []
    for i in range(num_procs):
        args.append([filename, i])
    mrt_stats_chunks = Pool.map(load_parse_mrt_rib, args)
    """
    #args = [filename, 0]
    #print(timeit.timeit('load_parse_mrt_rib()', 'from __main__ import load_parse_mrt_rib'))


    args = []
    for i in range(num_procs):
        args.append(filename + "_" + str(i))
    mrt_stats_chunks = Pool.map(load_parse_mrt_update, args)

    """
    # MOVE TO BE PRINT() FUNCION ON CLASS/OBJECT
    for rstats in mrt_stats_chunks:

        for re in rstats.most_origin_asns:
            print(f"most_origin_asns->prefix: {re.prefix}")
            print(f"most_origin_asns->as_path: {re.as_path}")
            print(f"most_origin_asns->community_set: {re.community_set}")
            print(f"most_origin_asns->next_hop: {re.next_hop}")
            print(f"most_origin_asns->origin_asns: {re.origin_asns}")    
        print("")

        for re in rstats.longest_as_path:
            print(f"longest_as_path->prefix: {re.prefix}")
            print(f"longest_as_path->as_path: {re.as_path}")
            print(f"longest_as_path->community_set: {re.community_set}")
            print(f"longest_as_path->next_hop: {re.next_hop}")
            print(f"longest_as_path->origin_asns: {re.origin_asns}")
        print("")

        for re in rstats.longest_community_set:
            print(f"longest_community_set->prefix: {re.prefix}")
            print(f"longest_community_set->as_path: {re.as_path}")
            print(f"longest_community_set->community_set: {re.community_set}")
            print(f"longest_community_set->next_hop: {re.next_hop}")
            print(f"longest_community_set->origin_asns: {re.origin_asns}")
        print("")

    print("")
    print("")
    """

    results = merge_chunks(mrt_stats_chunks)
    # ^ Add to results the filename these came from


    """
    # MOVE TO BE PRINT() FUNCION ON CLASS/OBJECT
    for re in results.most_origin_asns:
        print(f"most_origin_asns->prefix: {re.prefix}")
        print(f"most_origin_asns->as_path: {re.as_path}")
        print(f"most_origin_asns->community_set: {re.community_set}")
        print(f"most_origin_asns->next_hop: {re.next_hop}")
        print(f"most_origin_asns->origin_asns: {re.origin_asns}")    
    print("")

    for re in results.longest_as_path:
        print(f"longest_as_path->prefix: {re.prefix}")
        print(f"longest_as_path->as_path: {re.as_path}")
        print(f"longest_as_path->community_set: {re.community_set}")
        print(f"longest_as_path->next_hop: {re.next_hop}")
        print(f"longest_as_path->origin_asns: {re.origin_asns}")
    print("")

    for re in results.longest_community_set:
        print(f"longest_community_set->prefix: {re.prefix}")
        print(f"longest_community_set->as_path: {re.as_path}")
        print(f"longest_community_set->community_set: {re.community_set}")
        print(f"longest_community_set->next_hop: {re.next_hop}")
        print(f"longest_community_set->origin_asns: {re.origin_asns}")
    print("")

    """


    # MOVE TO BE PRINT() FUNCION ON CLASS/OBJECT
    for re in results.longest_as_path:
        print(f"longest_as_path->prefix: {re.prefix}")
        print(f"longest_as_path->advertisements: {re.advertisements}")
        print(f"longest_as_path->as_path: {re.as_path}")
        print(f"longest_as_path->community_set: {re.community_set}")
        print(f"longest_as_path->next_hop: {re.next_hop}")
        print(f"longest_as_path->origin_asns: {re.origin_asns}")
        print(f"longest_as_path->peer_asn: {re.peer_asn}")
        print(f"longest_as_path->timestamp: {re.timestamp}")
        print(f"longest_as_path->updates: {re.updates}")
        print(f"longest_as_path->withdraws: {re.withdraws}")
    print("")


    for re in results.longest_community_set:
        print(f"longest_community_set->prefix: {re.prefix}")
        print(f"longest_community_set->advertisements: {re.advertisements}")
        print(f"longest_community_set->as_path: {re.as_path}")
        print(f"longest_community_set->community_set: {re.community_set}")
        print(f"longest_community_set->next_hop: {re.next_hop}")
        print(f"longest_community_set->origin_asns: {re.origin_asns}")
        print(f"longest_community_set->peer_asn: {re.peer_asn}")
        print(f"longest_community_set->timestamp: {re.timestamp}")
        print(f"longest_community_set->updates: {re.updates}")
        print(f"longest_community_set->withdraws: {re.withdraws}")
    print("")

    for re in results.most_advt_prefixes:
        print(f"most_advt_prefixes->prefix: {re.prefix}")
        print(f"most_advt_prefixes->advertisements: {re.advertisements}")
        print(f"most_advt_prefixes->as_path: {re.as_path}")
        print(f"most_advt_prefixes->community_set: {re.community_set}")
        print(f"most_advt_prefixes->next_hop: {re.next_hop}")
        print(f"most_advt_prefixes->origin_asns: {re.origin_asns}")
        print(f"most_advt_prefixes->peer_asn: {re.peer_asn}")
        print(f"most_advt_prefixes->timestamp: {re.timestamp}")
        print(f"most_advt_prefixes->updates: {re.updates}")
        print(f"most_advt_prefixes->withdraws: {re.withdraws}")
    print("")


    for re in results.most_upd_prefixes:
        print(f"most_upd_prefixes->prefix: {re.prefix}")
        print(f"most_upd_prefixes->advertisements: {re.advertisements}")
        print(f"most_upd_prefixes->as_path: {re.as_path}")
        print(f"most_upd_prefixes->community_set: {re.community_set}")
        print(f"most_upd_prefixes->next_hop: {re.next_hop}")
        print(f"most_upd_prefixes->origin_asns: {re.origin_asns}")
        print(f"most_upd_prefixes->peer_asn: {re.peer_asn}")
        print(f"most_upd_prefixes->timestamp: {re.timestamp}")
        print(f"most_upd_prefixes->updates: {re.updates}")
        print(f"most_upd_prefixes->withdraws: {re.withdraws}")
    print("")


    for re in results.most_withd_prefixes:
        print(f"most_withd_prefixes->prefix: {re.prefix}")
        print(f"most_withd_prefixes->advertisements: {re.advertisements}")
        print(f"most_withd_prefixes->as_path: {re.as_path}")
        print(f"most_withd_prefixes->community_set: {re.community_set}")
        print(f"most_withd_prefixes->next_hop: {re.next_hop}")
        print(f"most_withd_prefixes->origin_asns: {re.origin_asns}")
        print(f"most_withd_prefixes->peer_asn: {re.peer_asn}")
        print(f"most_withd_prefixes->timestamp: {re.timestamp}")
        print(f"most_withd_prefixes->updates: {re.updates}")
        print(f"most_withd_prefixes->withdraws: {re.withdraws}")
    print("")

    for re in results.most_advt_origin_asn:
        print(f"most_advt_origin_asn->prefix: {re.prefix}")
        print(f"most_advt_origin_asn->advertisements: {re.advertisements}")
        print(f"most_advt_origin_asn->as_path: {re.as_path}")
        print(f"most_advt_origin_asn->community_set: {re.community_set}")
        print(f"most_advt_origin_asn->next_hop: {re.next_hop}")
        print(f"most_advt_origin_asn->origin_asns: {re.origin_asns}")
        print(f"most_advt_origin_asn->peer_asn: {re.peer_asn}")
        print(f"most_advt_origin_asn->timestamp: {re.timestamp}")
        print(f"most_advt_origin_asn->updates: {re.updates}")
        print(f"most_advt_origin_asn->withdraws: {re.withdraws}")
    print("")

    for re in results.most_advt_peer_asn:
        print(f"most_advt_peer_asn->prefix: {re.prefix}")
        print(f"most_advt_peer_asn->advertisements: {re.advertisements}")
        print(f"most_advt_peer_asn->as_path: {re.as_path}")
        print(f"most_advt_peer_asn->community_set: {re.community_set}")
        print(f"most_advt_peer_asn->next_hop: {re.next_hop}")
        print(f"most_advt_peer_asn->origin_asns: {re.origin_asns}")
        print(f"most_advt_peer_asn->peer_asn: {re.peer_asn}")
        print(f"most_advt_peer_asn->timestamp: {re.timestamp}")
        print(f"most_advt_peer_asn->updates: {re.updates}")
        print(f"most_advt_peer_asn->withdraws: {re.withdraws}")
    print("")

    for re in results.most_upd_peer_asn:
        print(f"most_upd_peer_asn->prefix: {re.prefix}")
        print(f"most_upd_peer_asn->advertisements: {re.advertisements}")
        print(f"most_upd_peer_asn->as_path: {re.as_path}")
        print(f"most_upd_peer_asn->community_set: {re.community_set}")
        print(f"most_upd_peer_asn->next_hop: {re.next_hop}")
        print(f"most_upd_peer_asn->origin_asns: {re.origin_asns}")
        print(f"most_upd_peer_asn->peer_asn: {re.peer_asn}")
        print(f"most_upd_peer_asn->timestamp: {re.timestamp}")
        print(f"most_upd_peer_asn->updates: {re.updates}")
        print(f"most_upd_peer_asn->withdraws: {re.withdraws}")
    print("")

    for re in results.most_withd_peer_asn:
        print(f"most_withd_peer_asn->prefix: {re.prefix}")
        print(f"most_withd_peer_asn->advertisements: {re.advertisements}")
        print(f"most_withd_peer_asn->as_path: {re.as_path}")
        print(f"most_withd_peer_asn->community_set: {re.community_set}")
        print(f"most_withd_peer_asn->next_hop: {re.next_hop}")
        print(f"most_withd_peer_asn->origin_asns: {re.origin_asns}")
        print(f"most_withd_peer_asn->peer_asn: {re.peer_asn}")
        print(f"most_withd_peer_asn->timestamp: {re.timestamp}")
        print(f"most_withd_peer_asn->updates: {re.updates}")
        print(f"most_withd_peer_asn->withdraws: {re.withdraws}")
    print("")

    for re in results.most_origin_asns:
        print(f"most_origin_asns->prefix: {re.prefix}")
        print(f"most_origin_asns->advertisements: {re.advertisements}")
        print(f"most_origin_asns->as_path: {re.as_path}")
        print(f"most_origin_asns->community_set: {re.community_set}")
        print(f"most_origin_asns->next_hop: {re.next_hop}")
        print(f"most_origin_asns->origin_asns: {re.origin_asns}")
        print(f"most_origin_asns->peer_asn: {re.peer_asn}")
        print(f"most_origin_asns->timestamp: {re.timestamp}")
        print(f"most_origin_asns->updates: {re.updates}")
        print(f"most_origin_asns->withdraws: {re.withdraws}")
    print("")


if __name__ == '__main__':
    main()
