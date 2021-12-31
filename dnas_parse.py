# RIB dumps are every 2 hours:
# http://archive.routeviews.org/route-views.linx/bgpdata/2021.12/RIBS/rib.20211222.0600.bz2
# UPDATES are ever 5 minutes:
# http://archive.routeviews.org/route-views.linx/bgpdata/2021.12/UPDATES/updates.20211222.0600.bz2

import mrtparse
import os
import requests

import timeit
import itertools
import operator

import math
import multiprocessing
from multiprocessing import Pool

class mrt_entry:

    def __init__(
        self,
        advertisements=0,
        as_path=[[]],
        community_set=[[]],
        next_hop=None,
        prefix=None,
        origin_asn=[],  ##### SHOULD THIS BE ORIGIN ASNS PLURAL?
        peer_asn=None,
        timestamp=None,
        updates=0,
        withdraws=0,
    ):

        self.advertisements = advertisements
        self.as_path = as_path
        self.community_set = community_set
        self.next_hop = next_hop
        self.origin_asn = origin_asn
        self.peer_asn = peer_asn
        self.prefix = prefix
        self.timestamp = timestamp
        self.updates = updates
        self.withdraws = withdraws

class mrt_stats:

    def __init__(self):
        self.longest_as_path = [mrt_entry()]
        self.longest_community_set = [mrt_entry()]
        self.most_advt_prefixes = [mrt_entry()]
        self.most_upd_prefixes = [mrt_entry()]
        self.most_withd_prefixes = [mrt_entry()]
        self.most_advt_origin_asn = [mrt_entry()]
        self.most_advt_peer_asn = [mrt_entry()]
        self.most_upd_peer_asn = [mrt_entry()]
        self.most_withd_peer_asn = [mrt_entry()]
        self.most_origin_asns = [mrt_entry()]

def download_mrt(filename, url):

    with open(filename, "wb") as f:
        print(f"Downloading {url} to {filename}")
        try:
            req = requests.get(url, stream=True)
        except requests.exceptions.ConnectionError as e:
            print(f"Couldn't connect to MRT server: {e}")
            f.close()
            return False

        file_len = req.headers['Content-length']

        if req.status_code != 200:
            print(f"HTTP error: {req.status_code}")
            print(req.url)
            print(req.text)
            f.write(req.content)
            f.close()
            return False

        if file_len is None:
            print(f"Missing file length!")
            print(req.url)
            print(req.text)
            f.write(req.content)
            f.close()
            return False

        file_len = int(file_len)
        rcvd = 0
        print(f"File size is {file_len/1024/1024:.7}MBs")
        progress = 0
        for chunk in req.iter_content(chunk_size=1024):

            if req.status_code != 200:
                print(f"HTTP error: {req.status_code}")
                print(req.url)
                print(req.text)
                f.write(req.content)
                f.close()
                return False

            rcvd += len(chunk)
            f.write(chunk)
            f.flush()

            if rcvd == file_len:
                print(f"Downloaded {rcvd}/{file_len} ({(rcvd/file_len)*100}%)")
            elif ((rcvd/file_len)*100)//10 > progress:
                print(f"\rDownloaded {rcvd}/{file_len} ({(rcvd/file_len)*100:.3}%)", end="\r")
                progress = ((rcvd/file_len)*100)//10

    return True

def load_parse_mrt_rib(args=None):

    print(f"Started PID {os.getpid()} with args {args}")

    # RIB dumps can contain both AFIs (v4 and v6)
    rstats = mrt_stats()

    #filename = args[0] + "_" + str(args[1])
    filename = "/home/bensley/GitHub/dfz_rust/ribv6.20211222.0600_0"
    print(f"PID {os.getpid()} is opening {filename}")
    entries = mrtparse.Reader(filename)

    tic = timeit.default_timer()
    for idx, entry in enumerate(entries):

        origin_asns = []
        longest_as_path = [mrt_entry()]
        longest_community_set = [mrt_entry()]
        prefix = entry.data["prefix"] + "/" + str(entry.data["prefix_length"])

        #if (entry["type"][0] == mrtparse.MRT_T['TABLE_DUMP_V2'] and
        ###if entry["subtype"][0] == mrtparse.TD_V2_ST['RIB_IPV4_UNICAST']:

        for re in entry.data["rib_entries"]:

            as_path = []
            origin_asn = None
            community_set = []
            next_hop = None

            for attr in re["path_attributes"]:
                at = attr["type"][0]

                if at == 2: # attr["type"][0] == 2: # mrtparse.BGP_ATTR_T['AS_PATH']:
                    as_path = attr["value"][0]["value"]
                    origin_asn = as_path[-1]
                    if origin_asn not in origin_asns:
                        origin_asns.append(origin_asn)

                #elif (attr["type"][0] == 8 or # mrtparse.BGP_ATTR_T['COMMUNITY'] or
                #    attr["type"][0] == 32): # mrtparse.BGP_ATTR_T['LARGE_COMMUNITY']):
                elif (at == 8 or at == 32):
                    community_set = attr["value"]

                elif at == 3: # attr["type"][0] == 3: # mrtparse.BGP_ATTR_T['NEXT_HOP']:
                    next_hop = attr["value"]

                elif at == 14: # attr["type"][0] == 14: # mrtparse.BGP_ATTR_T['MP_REACH_NLRI']:
                    next_hop = attr["value"]["next_hop"]

            if len(as_path) == len(longest_as_path[0].as_path):
                longest_as_path.append(
                    mrt_entry(
                        prefix=prefix,
                        as_path=as_path,
                        community_set=community_set,
                        next_hop=next_hop,
                        origin_asn=origin_asn,
                    )
                )
            elif len(as_path) > len(longest_as_path[0].as_path):
                longest_as_path = [
                    mrt_entry(
                        prefix=prefix,
                        as_path=as_path,
                        community_set=community_set,
                        next_hop=next_hop,
                        origin_asn=origin_asn,
                    )
                ]

            if len(community_set) == len(longest_community_set[0].community_set):
                longest_community_set.append(
                    mrt_entry(
                        prefix=prefix,
                        as_path=as_path,
                        community_set=community_set,
                        next_hop=next_hop,
                        origin_asn=origin_asn,
                    )
                )
            elif len(community_set) > len(longest_community_set[0].community_set):
                longest_community_set = [
                    mrt_entry(
                        prefix=prefix,
                        as_path=as_path,
                        community_set=community_set,
                        next_hop=next_hop,
                        origin_asn=origin_asn,
                    )
                ]

        if len(longest_as_path[0].as_path) == len(rstats.longest_as_path[0].as_path):
            rstats.longest_as_path.extend(longest_as_path)
        elif len(longest_as_path[0].as_path) > len(rstats.longest_as_path[0].as_path):
            rstats.longest_as_path = longest_as_path.copy()

        if len(longest_community_set[0].community_set) == len(rstats.longest_community_set[0].community_set):
            rstats.longest_community_set.extend(longest_community_set)
        elif len(longest_community_set[0].community_set) > len(rstats.longest_community_set[0].community_set):
            rstats.longest_community_set = longest_community_set.copy()

        if len(origin_asns) == len(rstats.most_origin_asns[0].origin_asn):
            rstats.most_origin_asns.append(
                mrt_entry(
                    prefix = prefix,
                    origin_asn = origin_asns,
                )
            )
        elif len(origin_asns) > len(rstats.most_origin_asns[0].origin_asn):
            rstats.most_origin_asns = [
                mrt_entry(
                    prefix = prefix,
                    origin_asn = origin_asns,
                )
            ]

        # Is there a noticable performance hit to wrap in a "try" ?
        #else:
        #    print(f"Unknown type/subtype: {entry['type']}/{entry['subtype']}")

    toc = timeit.default_timer()
    print(f"PID {os.getpid()} duration: {toc - tic}")

    return rstats

def load_parse_mrt_update(filename):

    print(f"Started PID {os.getpid()} with filename {filename}")

    # Update dumps can contain both AFIs (v4 and v6)
    stats = mrt_stats()
    longest_as_path = [mrt_entry()]
    longest_community_set = [mrt_entry()]
    origin_asns = {}
    upd_prefix = {}
    advt_per_origin_asn = {}
    upd_peer_asn = {}

    entries = mrtparse.Reader(filename)

    tic = timeit.default_timer()
    for idx, entry in enumerate(entries):

        peer_asn = entry.data["peer_as"]
        if peer_asn not in upd_peer_asn:
            upd_peer_asn[peer_asn] = {
                "advertisements": 0,
                "withdraws": 0,
            }

        timestamp = entry.data["timestamp"]

        as_path = []
        community_set = []

        if len(entry.data["bgp_message"]["withdrawn_routes"]) > 0:
            upd_peer_asn[peer_asn]["withdraws"] += 1

            for withdrawn_route in entry.data["bgp_message"]["withdrawn_routes"]:
                prefix = withdrawn_route["prefix"] + "/" + str(withdrawn_route["prefix_length"])
                if prefix not in upd_prefix:
                    upd_prefix[prefix] = {
                        "advertisements": 0,
                        "withdraws": 1,
                    }
                    origin_asns[prefix] = []
                else:
                    upd_prefix[prefix]["withdraws"] += 1

        if len(entry.data["bgp_message"]["path_attributes"]) > 1:
            upd_peer_asn[peer_asn]["advertisements"] += 1
            prefixes = []

            for path_attr in entry.data["bgp_message"]["path_attributes"]:
                attr_t = path_attr["type"][0]

                # AS_PATH
                if attr_t == 2:
                    as_path = path_attr["value"][0]["value"]
                    origin_asn = as_path[-1]
                    if origin_asn not in advt_per_origin_asn:
                        advt_per_origin_asn[origin_asn] = 1
                    else:
                        advt_per_origin_asn[origin_asn] += 1

                # NEXT_HOP
                elif attr_t == 3:
                    next_hop = path_attr["value"]

                # COMMUNITY or LARGE_COMMUNITY
                elif (attr_t == 8 or attr_t == 32):
                    community_set = path_attr["value"]

                # MP_REACH_NLRI
                elif attr_t == 14:
                    next_hop = path_attr["value"]["next_hop"]
                    for nlri in path_attr["value"]["nlri"]:
                        prefixes.append(nlri["prefix"] + "/" + str(nlri["prefix_length"]))

            for nlri in entry.data["bgp_message"]["nlri"]:
                prefixes.append(nlri["prefix"] + "/" + str(nlri["prefix_length"]))

            for prefix in prefixes:

                if prefix not in upd_prefix:
                    upd_prefix[prefix] = {
                        "advertisements": 1,
                        "withdraws": 0,
                    }
                    origin_asns[prefix] = [origin_asn]
                else:
                    upd_prefix[prefix]["advertisements"] += 1
                    if origin_asn not in origin_asns[prefix]:
                        origin_asns[prefix].append(origin_asn) ##### Change lists to set?


        if len(as_path) == len(longest_as_path[0].as_path):
            for prefix in prefixes:
                longest_as_path.append(
                    mrt_entry(
                        as_path=as_path,
                        community_set=community_set,
                        next_hop=next_hop,
                        origin_asn=origin_asn,
                        peer_asn=peer_asn,
                        prefix=prefix,
                        timestamp=timestamp,
                    )
                )

        elif len(as_path) > len(longest_as_path[0].as_path):
            longest_as_path = []
            for prefix in prefixes:
                longest_as_path.append(
                    mrt_entry(
                        as_path=as_path,
                        community_set=community_set,
                        next_hop=next_hop,
                        origin_asn=origin_asn,
                        peer_asn=peer_asn,
                        prefix=prefix,
                        timestamp=timestamp,
                    )
                )

        if len(community_set) == len(longest_community_set[0].community_set):
            for prefix in prefixes:
                longest_community_set.append(
                    mrt_entry(
                        as_path=as_path,
                        community_set=community_set,
                        next_hop=next_hop,
                        origin_asn=origin_asn,
                        peer_asn=peer_asn,
                        prefix=prefix,
                        timestamp=timestamp,
                    )
                )

        elif len(community_set) > len(longest_community_set[0].community_set):
            for prefix in prefixes:
                longest_community_set = []
                longest_community_set.append(
                    mrt_entry(
                        as_path=as_path,
                        community_set=community_set,
                        next_hop=next_hop,
                        origin_asn=origin_asn,
                        peer_asn=peer_asn,
                        prefix=prefix,
                        timestamp=timestamp,
                    )
                )

    if len(longest_as_path[0].as_path) == len(stats.longest_as_path[0].as_path):
        s_paths = [mrt_e.as_path for mrt_e in stats.longest_as_path]
        u_paths = [mrt_e.as_path for mrt_e in longest_as_path]
        for u_path in u_paths:
            if u_path not in s_paths:
                stats.longest_as_path.extend(u_path)
    elif len(longest_as_path[0].as_path) > len(stats.longest_as_path[0].as_path):
        stats.longest_as_path = longest_as_path.copy()

    if len(longest_community_set[0].community_set) == len(stats.longest_community_set[0].community_set):
        s_comms = [mrt_e.community_set for mrt_e in stats.longest_community_set]
        u_comms = [mrt_e.community_set for mrt_e in longest_community_set]
        for u_comm in u_comms:
            if u_comm not in s_comms:
                stats.longest_community_set.extend(u_comm)
    elif len(longest_community_set[0].community_set) > len(stats.longest_community_set[0].community_set):
        stats.longest_community_set = longest_community_set.copy()

    for prefix in upd_prefix:
        if (upd_prefix[prefix]["advertisements"] == stats.most_advt_prefixes[0].advertisements and
            stats.most_advt_prefixes[0].advertisements > 0):
            stats.most_advt_prefixes.append(
                mrt_entry(
                    prefix=prefix,
                    advertisements=upd_prefix[prefix]["advertisements"],
                )
            )
        elif upd_prefix[prefix]["advertisements"] > stats.most_advt_prefixes[0].advertisements:
            stats.most_advt_prefixes = [
                mrt_entry(
                    prefix=prefix,
                    advertisements=upd_prefix[prefix]["advertisements"],
                )
            ]

    for prefix in upd_prefix:
        if (upd_prefix[prefix]["withdraws"] == stats.most_withd_prefixes[0].withdraws and
            stats.most_withd_prefixes[0].withdraws > 0):
            stats.most_withd_prefixes.append(
                mrt_entry(
                    prefix=prefix,
                    withdraws=upd_prefix[prefix]["withdraws"],
                )
            )
        elif upd_prefix[prefix]["withdraws"] > stats.most_withd_prefixes[0].withdraws:
            stats.most_withd_prefixes = [
                mrt_entry(
                    prefix=prefix,
                    withdraws=upd_prefix[prefix]["withdraws"],
                )
            ]

    max_updates = 0
    max_prefixes = []
    for prefix in upd_prefix:
        if (upd_prefix[prefix]["advertisements"] + upd_prefix[prefix]["withdraws"]) > max_updates:
            max_updates = (upd_prefix[prefix]["advertisements"] + upd_prefix[prefix]["withdraws"])
            max_prefixes = [prefix]
        elif (upd_prefix[prefix]["advertisements"] + upd_prefix[prefix]["withdraws"]) == max_updates:
            max_prefixes.append(prefix)

    stats.most_upd_prefixes = [
        mrt_entry(
            prefix=prefix,
            updates=max_updates,
        ) for prefix in max_prefixes
    ]


    for asn in upd_peer_asn:
        if (upd_peer_asn[asn]["advertisements"] == stats.most_advt_peer_asn[0].advertisements and
            stats.most_advt_peer_asn[0].advertisements > 0):
            stats.most_advt_peer_asn.append(
                mrt_entry(
                    peer_asn=asn,
                    advertisements=upd_peer_asn[asn]["advertisements"],
                )
            )
        elif upd_peer_asn[asn]["advertisements"] > stats.most_advt_peer_asn[0].advertisements:
            stats.most_advt_peer_asn = [
                mrt_entry(
                    peer_asn=asn,
                    advertisements=upd_peer_asn[asn]["advertisements"],
                )
            ]

    for asn in upd_peer_asn:
        if (upd_peer_asn[asn]["withdraws"] == stats.most_withd_peer_asn[0].withdraws and
            stats.most_withd_peer_asn[0].withdraws > 0):
            stats.most_withd_peer_asn.append(
                mrt_entry(
                    peer_asn=asn,
                    withdraws=upd_peer_asn[asn]["withdraws"],
                )
            )
        elif upd_peer_asn[asn]["withdraws"] > stats.most_withd_peer_asn[0].withdraws:
            stats.most_withd_peer_asn = [
                mrt_entry(
                    peer_asn=asn,
                    withdraws=upd_peer_asn[asn]["withdraws"],
                )
            ]

    max_updates = 0
    max_asns = []
    for asn in upd_peer_asn:
        if (upd_peer_asn[asn]["advertisements"] + upd_peer_asn[asn]["withdraws"]) > max_updates:
            max_updates = (upd_peer_asn[asn]["advertisements"] + upd_peer_asn[asn]["withdraws"])
            max_asns = [asn]
        elif (upd_peer_asn[asn]["advertisements"] + upd_peer_asn[asn]["withdraws"]) == max_updates:
            max_asns.append(asn)

    stats.most_upd_peer_asn = [
        mrt_entry(
            peer_asn=asn,
            updates=max_updates,
        ) for asn in max_asns
    ]

    asns_length = 0
    asns = []
    for prefix in origin_asns:
        if len(origin_asns[prefix]) > asns_length:
            asns_length = len(origin_asns[prefix])
            asns = [(prefix, origin_asns[prefix])]
        elif len(origin_asns[prefix]) == asns_length:
            asns.append((prefix, origin_asns[prefix]))

    stats.most_origin_asns = [
        mrt_entry(
            prefix=x[0],
            origin_asn=x[1],
        ) for x in asns
    ]

    advt_per_origin_asn = sorted(advt_per_origin_asn.items(), key=operator.itemgetter(1))
    stats.most_advt_origin_asn = [
        mrt_entry(
            origin_asn=[x[0]],
            advertisements=x[1],
        ) for x in advt_per_origin_asn if x[1] == advt_per_origin_asn[-1][1]
    ]


        # Is there a noticable performance hit to wrap in a "try" ?
        #else:
        #    print(f"Unknown type/subtype: {entry['type']}/{entry['subtype']}")

    toc = timeit.default_timer()
    print(f"PID {os.getpid()} completed, duration: {toc - tic}, entires: {idx + 1}")

    return stats

def merge_chunks(mrt_stats_chunks):

    results = mrt_stats()

    for rstats in mrt_stats_chunks:

        if len(rstats.longest_as_path[0].as_path) == len(results.longest_as_path[0].as_path):
            s_paths = [mrt_e.as_path for mrt_e in results.longest_as_path]
            u_paths = [mrt_e.as_path for mrt_e in rstats.longest_as_path]
            for u_path in u_paths:
                if u_path not in s_paths:
                    results.longest_as_path.extend(u_path)
        elif len(rstats.longest_as_path[0].as_path) > len(results.longest_as_path[0].as_path):
            results.longest_as_path = rstats.longest_as_path.copy()

        if len(rstats.longest_community_set[0].community_set) == len(results.longest_community_set[0].community_set):
            s_comms = [mrt_e.community_set for mrt_e in results.longest_community_set]
            u_comms = [mrt_e.community_set for mrt_e in rstats.longest_community_set]
            for u_comm in u_comms:
                if u_comm not in s_comms:
                    results.longest_community_set.extend(u_comm)
        elif len(rstats.longest_community_set[0].community_set) > len(results.longest_community_set[0].community_set):
            results.longest_community_set = rstats.longest_community_set.copy()


        tmp = []
        for idx, u_e in enumerate(rstats.most_advt_prefixes):
            for res_e in results.most_advt_prefixes:
                if res_e.prefix == u_e.prefix:
                    print(res_e)
                    print(res_e.advertisements)
                    print(u_e)
                    print(u_e.advertisements)
                    tmp.append(
                        mrt_entry(
                            prefix=res_e.prefix,
                            advertisements=(res_e.advertisements + u_e.advertisements),
                        )
                    )
                    print(f"{res_e.prefix} now has {(res_e.advertisements + u_e.advertisements)}")
                    print(f"Deleting: {rstats.most_advt_prefixes[idx]}")
                    del(rstats.most_advt_prefixes[idx])

        if rstats.most_advt_prefixes:
            if (rstats.most_advt_prefixes[0].advertisements == results.most_advt_prefixes[0].advertisements and
                results.most_advt_prefixes[0].advertisements > 0):
                results.most_advt_prefixes.extend(rstats.most_advt_prefixes)
            elif rstats.most_advt_prefixes[0].advertisements > results.most_advt_prefixes[0].advertisements:
                results.most_advt_prefixes = rstats.most_advt_prefixes.copy()

        for tmp_e in tmp:
            if tmp_e.advertisements == results.most_advt_prefixes[0].advertisements:
                results.most_advt_prefixes.extend(tmp_e)
            elif tmp_e.advertisements > results.most_advt_prefixes[0].advertisements:
                results.most_advt_prefixes = [tmp_e]

        if (rstats.most_upd_prefixes[0].updates == results.most_upd_prefixes[0].updates and
            results.most_upd_prefixes[0].updates > 0):
            results.most_upd_prefixes.extend(rstats.most_upd_prefixes)
        elif rstats.most_upd_prefixes[0].updates > results.most_upd_prefixes[0].updates:
            results.most_upd_prefixes = rstats.most_upd_prefixes.copy()

        if (rstats.most_withd_prefixes[0].withdraws == results.most_withd_prefixes[0].withdraws and
            results.most_withd_prefixes[0].withdraws > 0):
            results.most_withd_prefixes.extend(rstats.most_withd_prefixes)
        elif rstats.most_withd_prefixes[0].withdraws > results.most_withd_prefixes[0].withdraws:
            results.most_withd_prefixes = rstats.most_withd_prefixes.copy()

        if (rstats.most_advt_origin_asn[0].advertisements == results.most_advt_origin_asn[0].advertisements and
            results.most_advt_origin_asn[0].advertisements > 0):
            results.most_advt_origin_asn.extend(rstats.most_advt_origin_asn)
        elif rstats.most_advt_origin_asn[0].advertisements > results.most_advt_origin_asn[0].advertisements:
            results.most_advt_origin_asn = rstats.most_advt_origin_asn.copy()

        if (rstats.most_advt_peer_asn[0].advertisements == results.most_advt_peer_asn[0].advertisements and
            results.most_advt_peer_asn[0].advertisements > 0):
            results.most_advt_peer_asn.extend(rstats.most_advt_peer_asn)
        elif rstats.most_advt_peer_asn[0].advertisements > results.most_advt_peer_asn[0].advertisements:
            results.most_advt_peer_asn = rstats.most_advt_peer_asn.copy()

        if (rstats.most_upd_peer_asn[0].updates == results.most_upd_peer_asn[0].updates and
            results.most_upd_peer_asn[0].updates > 0):
            results.most_upd_peer_asn.extend(rstats.most_upd_peer_asn)
        elif rstats.most_upd_peer_asn[0].updates > results.most_upd_peer_asn[0].updates:
            results.most_upd_peer_asn = rstats.most_upd_peer_asn.copy()

        if (rstats.most_withd_peer_asn[0].withdraws == results.most_withd_peer_asn[0].withdraws and
            results.most_withd_peer_asn[0].withdraws > 0):
            results.most_withd_peer_asn.extend(rstats.most_withd_peer_asn)
        elif rstats.most_withd_peer_asn[0].withdraws > results.most_withd_peer_asn[0].withdraws:
            results.most_withd_peer_asn = rstats.most_withd_peer_asn.copy()

        if len(rstats.most_origin_asns[0].origin_asn) == len(results.most_origin_asns[0].origin_asn):
            results.most_origin_asns.extend(rstats.most_origin_asns)
        elif len(rstats.most_origin_asns[0].origin_asn) > len(results.most_origin_asns[0].origin_asn):
            results.most_origin_asns = rstats.most_origin_asns.copy()

    return results

def test_mrt_rib(data):

    print(f"Started test_mrt_rib() with {len(data)} entries")

    i = 0
    for entry in data:
        if (entry.data["type"][0] != mrtparse.MRT_T['TABLE_DUMP_V2']):
            print(f"Not TABLE_DUMP_V2")
            print(entry.data)
            return i
        
        if (entry.data["subtype"][0] != mrtparse.TD_V2_ST['PEER_INDEX_TABLE'] and
            entry.data["subtype"][0] != mrtparse.TD_V2_ST['RIB_IPV4_UNICAST'] and
            entry.data["subtype"][0] != mrtparse.TD_V2_ST['RIB_IPV6_UNICAST']):
            print(f"Not PEER_INDEX_TABLE or RIB_IPV4_UNICAST or RIB_IPV6_UNICAST")
            print(entry.data)
            return i

        i+= 1

    return i

def test_mrt_updates(data):

    print(f"Started test_mrt_updates() with {len(data)} entries")

    i = 0
    for entry in data:
        if (entry.data["type"][0] != mrtparse.MRT_T['BGP4MP_ET']):
            print(f"Not BGP4MP_ET")
            print(entry.data)
            return i
        
        if (entry.data["subtype"][0] != mrtparse.BGP4MP_ST['BGP4MP_MESSAGE_AS4'] and
            entry.data["subtype"][0] != mrtparse.BGP4MP_ST['BGP4MP_MESSAGE']):
            print(f"Not BGP4MP_MESSAGE or BGP4MP_MESSAGE_AS4")
            print(entry.data)
            return i

        i+= 1

    return i

def get_mrt_size(filename):

    i = 0
    for entry in mrtparse.Reader(filename):
        i += 1

    return i

def main():

    # Download todays MRTs files:
    #filename = "/tmp/rib.20211222.0600.bz2"
    #url = "http://archive.routeviews.org/route-views.linx/bgpdata/2021.12/RIBS/rib.20211222.0600.bz2"
    #if download_mrt(filename, url) != True:
    #    exit(1)
    #filename = "/tmp/updates.20211222.0600.bz2"
    #url = "http://archive.routeviews.org/route-views.linx/bgpdata/2021.12/UPDATES/updates.20211222.0600.bz2" # 263632 entries
    #if download_mrt(filename, url) != True:
    #    exit(1)

    """
    filename = "/mnt/c/Users/bensley/GitHub/dfz_name_and_shame/rib.20211222.0600.bz2"
    if not load_rib(filename):
        exit(1)
    """

    """
    filename = "/mnt/c/Users/bensley/GitHub/dfz_name_and_shame/ribv6.20211222.0600.bz2"
    if not load_rib(filename):
        exit(1)
    """

    """
    filename = "/mnt/c/Users/bensley/GitHub/dfz_name_and_shame/updates.20211222.0600.bz2" # 263632 entries
    if not load_update(filename):
        exit(1)
    """

    filename = "/home/bensley/GitHub/dfz_rust/updates.20211222.0600" # 263632 entries / 0m6s
    #filename = "/home/bensley/GitHub/dfz_rust/rib.20211222.0600.bz2" # 1092192 entries / 7m5s
    #filename = "/home/bensley/GitHub/dfz_rust/ribv6.20211222.0600.bz2" #  148015 entries / 0m54s
    #filename = "/home/bensley/GitHub/dfz_rust/ribv6.20211222.0600" #  148015 entries / 0m54s
    #filename = "/home/bensley/GitHub/dfz_rust/updatesv6.20211222.0615.bz2"

    if not filename:
        print("MRT filename missing")
        return False

    if not os.path.isfile(filename):
        print(f"Non-existing MRT filename {filename}")
        return False

    ##mrt_length = 148015 #get_mrt_size(filename)

    num_procs =  multiprocessing.cpu_count()
    Pool = multiprocessing.Pool(num_procs)

    """
    # 78 seconds
    chunks = [[] for i in range(0, num_procs)]
    tic = timeit.default_timer()
    entries = mrtparse.Reader(filename)
    next(entries) # Skip the peer table which is the first entry in the RIB dump
    for idx, entry in enumerate(entries):
        chunks[idx % num_procs].append(entry.data)

    toc = timeit.default_timer()
    print(f"Duration: {toc - tic}")
    print(f"No. of entries: {idx}, No. of chunks: {len(chunks)}, chunk size: {len(chunks[0])}, No. of procs: {num_procs}")

    try:
        entries.close()
    except StopIteration:
        pass

    del(entries)
    """


    """
    80 seconds
    chunks = [[] for i in range(0, num_procs)]
    tic = timeit.default_timer()

    entries2 = mrtparse.Reader(filename)
    next(entries2)
    it = iter(entries2)
    for idx, entry in enumerate(entries2):
        chunks[idx % num_procs].append(itertools.islice(it, 1))

    toc = timeit.default_timer()
    print(f"Duration: {toc - tic}")
    """


    """
    # 81 seconds
    i = 0
    entries2 = mrtparse.Reader(filename)
    next(entries2)
    tic = timeit.default_timer()
    for entry in entries2:
        i += 1
    toc = timeit.default_timer()
    print(f"Duration: {toc - tic}")
    print(f"{i} entries")
    """

    """
    # 81 seconds
    i = 0
    entries3 = mrtparse.Reader(filename)
    next(entries3)
    it3 = iter(entries3)
    tic = timeit.default_timer()
    for entry in it3:
        i += 1
    toc = timeit.default_timer()
    print(f"Duration: {toc - tic}")
    print(f"{i} entries")
    """

    """
    # cant be serialised!
    # 1.4 seconds
    chunks = []
    entries4 = mrtparse.Reader(filename)
    next(entries4) # Skip the peer table which is the first entry in the RIB dump
    it = iter(entries4)
    tic = timeit.default_timer()
    for i in range(0, num_procs):
        chunks.append(itertools.islice(it, 18502))
    toc = timeit.default_timer()
    print(f"Duration: {toc - tic}")
    print(f"No. of chunks: {len(chunks)}, chunk size: {18502}, No. of procs: {num_procs}")
    """

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
            print(f"most_origin_asns->origin_asn: {re.origin_asn}")    
        print("")

        for re in rstats.longest_as_path:
            print(f"longest_as_path->prefix: {re.prefix}")
            print(f"longest_as_path->as_path: {re.as_path}")
            print(f"longest_as_path->community_set: {re.community_set}")
            print(f"longest_as_path->next_hop: {re.next_hop}")
            print(f"longest_as_path->origin_asn: {re.origin_asn}")
        print("")

        for re in rstats.longest_community_set:
            print(f"longest_community_set->prefix: {re.prefix}")
            print(f"longest_community_set->as_path: {re.as_path}")
            print(f"longest_community_set->community_set: {re.community_set}")
            print(f"longest_community_set->next_hop: {re.next_hop}")
            print(f"longest_community_set->origin_asn: {re.origin_asn}")
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
        print(f"most_origin_asns->origin_asn: {re.origin_asn}")    
    print("")

    for re in results.longest_as_path:
        print(f"longest_as_path->prefix: {re.prefix}")
        print(f"longest_as_path->as_path: {re.as_path}")
        print(f"longest_as_path->community_set: {re.community_set}")
        print(f"longest_as_path->next_hop: {re.next_hop}")
        print(f"longest_as_path->origin_asn: {re.origin_asn}")
    print("")

    for re in results.longest_community_set:
        print(f"longest_community_set->prefix: {re.prefix}")
        print(f"longest_community_set->as_path: {re.as_path}")
        print(f"longest_community_set->community_set: {re.community_set}")
        print(f"longest_community_set->next_hop: {re.next_hop}")
        print(f"longest_community_set->origin_asn: {re.origin_asn}")
    print("")

    """


    # MOVE TO BE PRINT() FUNCION ON CLASS/OBJECT
    for re in results.longest_as_path:
        print(f"longest_as_path->prefix: {re.prefix}")
        print(f"longest_as_path->advertisements: {re.advertisements}")
        print(f"longest_as_path->as_path: {re.as_path}")
        print(f"longest_as_path->community_set: {re.community_set}")
        print(f"longest_as_path->next_hop: {re.next_hop}")
        print(f"longest_as_path->origin_asn: {re.origin_asn}")
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
        print(f"longest_community_set->origin_asn: {re.origin_asn}")
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
        print(f"most_advt_prefixes->origin_asn: {re.origin_asn}")
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
        print(f"most_upd_prefixes->origin_asn: {re.origin_asn}")
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
        print(f"most_withd_prefixes->origin_asn: {re.origin_asn}")
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
        print(f"most_advt_origin_asn->origin_asn: {re.origin_asn}")
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
        print(f"most_advt_peer_asn->origin_asn: {re.origin_asn}")
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
        print(f"most_upd_peer_asn->origin_asn: {re.origin_asn}")
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
        print(f"most_withd_peer_asn->origin_asn: {re.origin_asn}")
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
        print(f"most_origin_asns->origin_asn: {re.origin_asn}")
        print(f"most_origin_asns->peer_asn: {re.peer_asn}")
        print(f"most_origin_asns->timestamp: {re.timestamp}")
        print(f"most_origin_asns->updates: {re.updates}")
        print(f"most_origin_asns->withdraws: {re.withdraws}")
    print("")


if __name__ == '__main__':
    main()
