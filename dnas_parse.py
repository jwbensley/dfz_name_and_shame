# RIB dumps are every 2 hours:
# http://archive.routeviews.org/route-views.linx/bgpdata/2021.12/RIBS/rib.20211222.0600.bz2
# UPDATES are ever 5 minutes:
# http://archive.routeviews.org/route-views.linx/bgpdata/2021.12/UPDATES/updates.20211222.0600.bz2

import mrtparse
import os
import requests

import math
import multiprocessing
from multiprocessing import Pool

options = []

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


def load_rib(filename):

    # RIB dumps can contain both AFIs (v4 and v6)

    if not filename:
        print("Filename missing from load_rib() call!")
        return False

    if not os.path.isfile(filename):
        print(f"Nonexisting filename parsed to load_rib(): {filename}")
        return False

    peers = {}
    v4_routes = {}
    v6_routes = {}
    for entry in mrtparse.Reader(filename):
        if (entry.data["type"][0] == mrtparse.MRT_T['TABLE_DUMP_V2'] and
            entry.data["subtype"][0] == mrtparse.TD_V2_ST['PEER_INDEX_TABLE']):
            peers = entry.data["peer_entries"]

        elif (entry.data["type"][0] == mrtparse.MRT_T['TABLE_DUMP_V2'] and
            entry.data["subtype"][0] == mrtparse.TD_V2_ST['RIB_IPV4_UNICAST']):
            prefix = entry.data["prefix"] + "/" + str(entry.data["prefix_length"])
            v4_routes[prefix] = {
                "updates": 0,
                "origin_asns": [],
                "longest_as_paths": [],
                "longest_community_sets": [],
            }
            """
            for rib_entry in entry.data["rib_entries"]:
                as_path = [
                    attr["value"][0]["value"]
                    for attr in rib_entry["path_attributes"]
                    if attr["type"][0] == mrtparse.BGP_ATTR_T['AS_PATH']
                ][0]

                community_set = [
                    attr["value"][0]
                    for attr in rib_entry["path_attributes"]
                    if (attr["type"][0] == mrtparse.BGP_ATTR_T['COMMUNITY'] or
                        attr["type"][0] == mrtparse.BGP_ATTR_T['LARGE_COMMUNITY'])
                ]

                if not v4_routes[prefix]["longest_as_paths"]:
                    v4_routes[prefix]["longest_as_paths"] = [as_path]
                else:
                    if len(as_path) == len(v4_routes[prefix]["longest_as_paths"][0]):
                        v4_routes[prefix]["longest_as_paths"].append(as_path)
                    elif len(as_path) > len(v4_routes[prefix]["longest_as_paths"][0]):
                        v4_routes[prefix]["longest_as_paths"] = [as_path]

                origin_asn = as_path[-1]
                if origin_asn not in v4_routes[prefix]["origin_asns"]:
                    v4_routes[prefix]["origin_asns"].append(origin_asn)

                if len(community_set) > 0:
                    if not v4_routes[prefix]["longest_community_sets"]:
                        v4_routes[prefix]["longest_community_sets"] = [(community_set, origin_asn)]
                    else:
                        if len(community_set) == len(v4_routes[prefix]["longest_community_sets"][0]):
                            v4_routes[prefix]["longest_community_sets"].append((community_set, origin_asn))
                        elif len(community_set) > len(v4_routes[prefix]["longest_community_sets"][0]):
                            v4_routes[prefix]["longest_community_sets"] = [(community_set, origin_asn)]
                """

        elif (entry.data["type"][0] == mrtparse.MRT_T['TABLE_DUMP_V2'] and
            entry.data["subtype"][0] == mrtparse.TD_V2_ST['RIB_IPV6_UNICAST']):
            prefix = entry.data["prefix"] + "/" + str(entry.data["prefix_length"])
            v6_routes[prefix] = {
                "updates": 0,
                "origin_asns": [],
                "longest_as_paths": [],
                "longest_community_sets": [],
            }
            """
            for rib_entry in entry.data["rib_entries"]:
                as_path = [
                    attr["value"][0]["value"]
                    for attr in rib_entry["path_attributes"]
                    if attr["type"][0] == mrtparse.BGP_ATTR_T['AS_PATH']
                ][0]

                community_set = [
                    attr["value"][0]
                    for attr in rib_entry["path_attributes"]
                    if (attr["type"][0] == mrtparse.BGP_ATTR_T['COMMUNITY'] or
                        attr["type"][0] == mrtparse.BGP_ATTR_T['LARGE_COMMUNITY'])
                ]

                if not v6_routes[prefix]["longest_as_paths"]:
                    v6_routes[prefix]["longest_as_paths"] = [as_path]
                else:
                    if len(as_path) == len(v6_routes[prefix]["longest_as_paths"][0]):
                        v6_routes[prefix]["longest_as_paths"].append(as_path)
                    elif len(as_path) > len(v6_routes[prefix]["longest_as_paths"][0]):
                        v6_routes[prefix]["longest_as_paths"] = [as_path]

                origin_asn = as_path[-1]
                if origin_asn not in v6_routes[prefix]["origin_asns"]:
                    v6_routes[prefix]["origin_asns"].append(origin_asn)

                if len(community_set) > 0:
                    if not v6_routes[prefix]["longest_community_sets"]:
                        v6_routes[prefix]["longest_community_sets"] = [(community_set, origin_asn)]
                    else:
                        if len(community_set) == len(v6_routes[prefix]["longest_community_sets"][0]):
                            v6_routes[prefix]["longest_community_sets"].append((community_set, origin_asn))
                        elif len(community_set) > len(v6_routes[prefix]["longest_community_sets"][0]):
                            v6_routes[prefix]["longest_community_sets"] = [(community_set, origin_asn)]
            """

        #else:
        #    print(f"Unknown type/subtype: {entry.data['type']}/{entry.data['subtype']}")

    print(f"Loaded {len(peers)} peers, {len(v4_routes)} IPv4 routes and {len(v6_routes)} IPv6 routes")
    return True


def load_update(filename):

    if not filename:
        print("Filename missing from load_update() call!")
        return False

    if not os.path.isfile(filename):
        print(f"Nonexisting filename parsed to load_update(): {filename}")
        return False

    peers = {}
    v4_routes = {}
    v6_routes = {}
    for entry in mrtparse.Reader(filename):
        if (entry.data["type"][0] == mrtparse.MRT_T['BGP4MP_ET'] and
            entry.data["subtype"][0] == mrtparse.BGP4MP_ST['BGP4MP_MESSAGE_AS4']):
            prefix = entry.data["prefix"] + "/" + str(entry.data["prefix_length"])
            if prefix not in v4_routes:
                v4_routes[prefix] = {
                    "updates": 0,
                    "origin_asns": [],
                    "longest_as_paths": [],
                    "longest_community_sets": [],
                }
                """
                for rib_entry in entry.data["rib_entries"]:
                    as_path = [
                        attr["value"][0]["value"]
                        for attr in rib_entry["path_attributes"]
                        if attr["type"][0] == mrtparse.BGP_ATTR_T['AS_PATH']
                    ][0]

                    community_set = [
                        attr["value"][0]
                        for attr in rib_entry["path_attributes"]
                        if (attr["type"][0] == mrtparse.BGP_ATTR_T['COMMUNITY'] or
                            attr["type"][0] == mrtparse.BGP_ATTR_T['LARGE_COMMUNITY'])
                    ]

                    if not v4_routes[prefix]["longest_as_paths"]:
                        v4_routes[prefix]["longest_as_paths"] = [as_path]
                    else:
                        if len(as_path) == len(v4_routes[prefix]["longest_as_paths"][0]):
                            v4_routes[prefix]["longest_as_paths"].append(as_path)
                        elif len(as_path) > len(v4_routes[prefix]["longest_as_paths"][0]):
                            v4_routes[prefix]["longest_as_paths"] = [as_path]

                    origin_asn = as_path[-1]
                    if origin_asn not in v4_routes[prefix]["origin_asns"]:
                        v4_routes[prefix]["origin_asns"].append(origin_asn)

                    if len(community_set) > 0:
                        if not v4_routes[prefix]["longest_community_sets"]:
                            v4_routes[prefix]["longest_community_sets"] = [(community_set, origin_asn)]
                        else:
                            if len(community_set) == len(v4_routes[prefix]["longest_community_sets"][0]):
                                v4_routes[prefix]["longest_community_sets"].append((community_set, origin_asn))
                            elif len(community_set) > len(v4_routes[prefix]["longest_community_sets"][0]):
                                v4_routes[prefix]["longest_community_sets"] = [(community_set, origin_asn)]
                """

        elif (entry.data["type"][0] == mrtparse.MRT_T['BGP4MP_ET'] and
            entry.data["subtype"][0] == mrtparse.BGP4MP_ST['BGP4MP_MESSAGE_AS4']):
            prefix = entry.data["prefix"] + "/" + str(entry.data["prefix_length"])
            if prefix not in v6_routes:
                v6_routes[prefix] = {
                    "updates": 0,
                    "origin_asns": [],
                    "longest_as_paths": [],
                    "longest_community_sets": [],
                }
                """
                for rib_entry in entry.data["rib_entries"]:
                    as_path = [
                        attr["value"][0]["value"]
                        for attr in rib_entry["path_attributes"]
                        if attr["type"][0] == mrtparse.BGP_ATTR_T['AS_PATH']
                    ][0]

                    community_set = [
                        attr["value"][0]
                        for attr in rib_entry["path_attributes"]
                        if (attr["type"][0] == mrtparse.BGP_ATTR_T['COMMUNITY'] or
                            attr["type"][0] == mrtparse.BGP_ATTR_T['LARGE_COMMUNITY'])
                    ]

                    if not v6_routes[prefix]["longest_as_paths"]:
                        v6_routes[prefix]["longest_as_paths"] = [as_path]
                    else:
                        if len(as_path) == len(v6_routes[prefix]["longest_as_paths"][0]):
                            v6_routes[prefix]["longest_as_paths"].append(as_path)
                        elif len(as_path) > len(v6_routes[prefix]["longest_as_paths"][0]):
                            v6_routes[prefix]["longest_as_paths"] = [as_path]

                    origin_asn = as_path[-1]
                    if origin_asn not in v6_routes[prefix]["origin_asns"]:
                        v6_routes[prefix]["origin_asns"].append(origin_asn)

                    if len(community_set) > 0:
                        if not v6_routes[prefix]["longest_community_sets"]:
                            v6_routes[prefix]["longest_community_sets"] = [(community_set, origin_asn)]
                        else:
                            if len(community_set) == len(v6_routes[prefix]["longest_community_sets"][0]):
                                v6_routes[prefix]["longest_community_sets"].append((community_set, origin_asn))
                            elif len(community_set) > len(v6_routes[prefix]["longest_community_sets"][0]):
                                v6_routes[prefix]["longest_community_sets"] = [(community_set, origin_asn)]
                """

        #else:
        #    print(f"Unknown type/subtype: {entry.data['type']}/{entry.data['subtype']}")

    print(f"Loaded {len(peers)} peers, {len(v4_routes)} IPv4 routes and {len(v6_routes)} IPv6 routes")
    return True


def test_rib(data):

    print(f"Started test_rib() with {len(data)} entries")

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


def test_updates(data):

    print(f"Started test_updates() with {len(data)} entries")

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

    #filename = "/mnt/c/Users/bensley/GitHub/dfz_name_and_shame/updates.20211222.0600.bz2" # 263632 entries / 0m6s
    #filename = "/mnt/c/Users/bensley/GitHub/dfz_name_and_shame/rib.20211222.0600.bz2" # 1092192 entries / 7m5s
    filename = "/mnt/c/Users/bensley/GitHub/dfz_name_and_shame/ribv6.20211222.0600.bz2" #  148015 entries / 0m54s
    mrt_length = 148015 #get_mrt_size(filename)

    data = []
    for entry in mrtparse.Reader(filename):
        data.append(entry)
    print(f"Data list if {len(data)} entries")

    num_proc = multiprocessing.cpu_count() - 1 ##############
    Pool = multiprocessing.Pool(num_proc)
    chunk_size = int(math.ceil(len(data) / num_proc))

    chunks = [
        data[i : i + chunk_size]
        for i in range(0, len(data), chunk_size)
    ]
    print(f"No of chunks: {len(chunks)}, chunk size: {chunk_size}, no of procs: {num_proc}")

    print(f"Starting chunks 0-{num_proc}...")
    #route_chunks = Pool.map(test_updates, chunks)
    route_chunks = Pool.map(test_rib, chunks)

    print(f"Number of chunks: {len(route_chunks)}")
    print(f"Results: {route_chunks}")


if __name__ == '__main__':
    main()
