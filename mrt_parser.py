import errno
import os
import mrtparse

class mrt_parser:
    """
    Class which provides various MRT file features such as parsing and testing.
    """

    @staticmethod
    def load_rib_dump(filename):

        print(f"Started PID {os.getpid()} with {filename}")

        rib_data = mrt_data()
        mrt_entries = mrtparse.Reader(filename)

        #############tic = timeit.default_timer()
        for idx, mrt_entry in enumerate(mrt_entries):

            origin_asns = set()
            longest_as_path = [mrt_entry()]
            longest_community_set = [mrt_entry()]
            prefix = mrt_entry.data["prefix"] + "/" + str(mrt_entry.data["prefix_length"])

            for rib_entry in mrt_entry.data["rib_entries"]:

                as_path = []
                origin_asn = None
                community_set = []
                next_hop = None

                for attr in rib_entry["path_attributes"]:
                    attr_t = attr["type"][0]

                    # mrtparse.BGP_ATTR_T['AS_PATH']
                    if attr_t == 2:
                        as_path = attr["value"][0]["value"]
                        origin_asn = as_path[-1]
                        origin_asns.add(origin_asn)
                    
                    # mrtparse.BGP_ATTR_T['COMMUNITY'] or
                    # mrtparse.BGP_ATTR_T['LARGE_COMMUNITY']
                    elif (attr_t == 8 or attr_t == 32):
                        community_set = attr["value"]

                    # mrtparse.BGP_ATTR_T['NEXT_HOP']
                    elif attr_t == 3:
                        next_hop = attr["value"]

                    # mrtparse.BGP_ATTR_T['MP_REACH_NLRI']
                    elif attr_t == 14:
                        next_hop = attr["value"]["next_hop"]

                if len(as_path) == len(longest_as_path[0].as_path):
                    longest_as_path.append(
                        mrt_entry(
                            prefix=prefix,
                            as_path=as_path,
                            community_set=community_set,
                            next_hop=next_hop,
                            origin_asns=set([origin_asn]),
                        )
                    )
                elif len(as_path) > len(longest_as_path[0].as_path):
                    longest_as_path = [
                        mrt_entry(
                            prefix=prefix,
                            as_path=as_path,
                            community_set=community_set,
                            next_hop=next_hop,
                            origin_asns=set([origin_asn]),
                        )
                    ]

                if len(community_set) == len(longest_community_set[0].community_set):
                    longest_community_set.append(
                        mrt_entry(
                            prefix=prefix,
                            as_path=as_path,
                            community_set=community_set,
                            next_hop=next_hop,
                            origin_asns=set([origin_asn]),
                        )
                    )
                elif len(community_set) > len(longest_community_set[0].community_set):
                    longest_community_set = [
                        mrt_entry(
                            prefix=prefix,
                            as_path=as_path,
                            community_set=community_set,
                            next_hop=next_hop,
                            origin_asns=set([origin_asn]),
                        )
                    ]

            if len(longest_as_path[0].as_path) == len(rib_data.longest_as_path[0].as_path):
                rib_data.longest_as_path.extend(longest_as_path)
            elif len(longest_as_path[0].as_path) > len(rib_data.longest_as_path[0].as_path):
                rib_data.longest_as_path = longest_as_path.copy()

            if len(longest_community_set[0].community_set) == len(rib_data.longest_community_set[0].community_set):
                rib_data.longest_community_set.extend(longest_community_set)
            elif len(longest_community_set[0].community_set) > len(rib_data.longest_community_set[0].community_set):
                rib_data.longest_community_set = longest_community_set.copy()

            if len(origin_asns) == len(rib_data.most_origin_asns[0].origin_asns):
                rib_data.most_origin_asns.append(
                    mrt_entry(
                        prefix = prefix,
                        origin_asns = origin_asns,
                    )
                )
            elif len(origin_asns) > len(rib_data.most_origin_asns[0].origin_asns):
                rib_data.most_origin_asns = [
                    mrt_entry(
                        prefix = prefix,
                        origin_asns = origin_asns,
                    )
                ]

            # Is there a noticable performance hit to wrap in a "try" ?
            #else:
            #    print(f"Unknown type/subtype: {entry['type']}/{entry['subtype']}")

        ####toc = timeit.default_timer()
        ####print(f"PID {os.getpid()} duration: {toc - tic}")

        return rib_data

    @staticmethod
    def load_update_dump(filename):

        print(f"Started PID {os.getpid()} with filename {filename}")

        upd_stats = mrt_stats()
        longest_as_path = [mrt_entry()]
        longest_community_set = [mrt_entry()]
        origin_asns_prefix = {}
        upd_prefix = {}
        advt_per_origin_asn = {}
        upd_peer_asn = {}

        mrt_entries = mrtparse.Reader(filename)
        #####tic = timeit.default_timer()

        for idx, entry in enumerate(mrt_entries):

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
                        origin_asns_prefix[prefix] = set()
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
                            prefixes.append(
                                nlri["prefix"] + "/" + str(nlri["prefix_length"])
                            )

                for nlri in entry.data["bgp_message"]["nlri"]:
                    prefixes.append(nlri["prefix"] + "/" + str(nlri["prefix_length"]))

                for prefix in prefixes:

                    if prefix not in upd_prefix:
                        upd_prefix[prefix] = {
                            "advertisements": 1,
                            "withdraws": 0,
                        }
                        origin_asns_prefix[prefix] = set([origin_asn])
                    else:
                        upd_prefix[prefix]["advertisements"] += 1
                        origin_asns_prefix[prefix].add(origin_asn)


            if len(as_path) == len(longest_as_path[0].as_path):
                for prefix in prefixes:
                    longest_as_path.append(
                        mrt_entry(
                            as_path=as_path,
                            community_set=community_set,
                            next_hop=next_hop,
                            origin_asns=set([origin_asn]),
                            peer_asn=peer_asn,
                            prefix=prefix,
                            timestamp=timestamp,
                        )
                    )

            elif len(as_path) > len(longest_as_path[0].as_path):
                longest_as_path = [
                    mrt_entry(
                        as_path=as_path,
                        community_set=community_set,
                        next_hop=next_hop,
                        origin_asns=set([origin_asn]),
                        peer_asn=peer_asn,
                        prefix=prefix,
                        timestamp=timestamp,
                    ) for prefix in prefixes
                ]

            if len(community_set) == len(longest_community_set[0].community_set):
                for prefix in prefixes:
                    longest_community_set.append(
                        mrt_entry(
                            as_path=as_path,
                            community_set=community_set,
                            next_hop=next_hop,
                            origin_asns=set([origin_asn]),
                            peer_asn=peer_asn,
                            prefix=prefix,
                            timestamp=timestamp,
                        )
                    )

            elif len(community_set) > len(longest_community_set[0].community_set):
                longest_community_set = [
                    mrt_entry(
                        as_path=as_path,
                        community_set=community_set,
                        next_hop=next_hop,
                        origin_asns=set([origin_asn]),
                        peer_asn=peer_asn,
                        prefix=prefix,
                        timestamp=timestamp,
                    ) for prefix in prefixes
                ]


        ##### upd_stats should always be emtpy so remove this after tests are added !!!!!!!!!!!!
        """
        if len(longest_as_path[0].as_path) == len(upd_stats.longest_as_path[0].as_path):
            s_paths = [mrt_e.as_path for mrt_e in upd_stats.longest_as_path]
            u_paths = [mrt_e.as_path for mrt_e in longest_as_path]
            for u_path in u_paths:
                if u_path not in s_paths:
                    upd_stats.longest_as_path.extend(u_path)
        elif len(longest_as_path[0].as_path) > len(upd_stats.longest_as_path[0].as_path):
            upd_stats.longest_as_path = longest_as_path.copy()
        """
        if len(longest_as_path[0].as_path) > len(upd_stats.longest_as_path[0].as_path):
            upd_stats.longest_as_path = longest_as_path.copy()


        """
        if len(longest_community_set[0].community_set) == len(upd_stats.longest_community_set[0].community_set):
            s_comms = [mrt_e.community_set for mrt_e in upd_stats.longest_community_set]
            u_comms = [mrt_e.community_set for mrt_e in longest_community_set]
            for u_comm in u_comms:
                if u_comm not in s_comms:
                    upd_stats.longest_community_set.extend(u_comm)
        elif len(longest_community_set[0].community_set) > len(upd_stats.longest_community_set[0].community_set):
            upd_stats.longest_community_set = longest_community_set.copy()
        """
        if len(longest_community_set[0].community_set) > len(upd_stats.longest_community_set[0].community_set):
            upd_stats.longest_community_set = longest_community_set.copy()


        for prefix in upd_prefix:
            if (upd_prefix[prefix]["advertisements"] == upd_stats.most_advt_prefixes[0].advertisements and
                upd_stats.most_advt_prefixes[0].advertisements > 0):
                upd_stats.most_advt_prefixes.append(
                    mrt_entry(
                        prefix=prefix,
                        advertisements=upd_prefix[prefix]["advertisements"],
                    )
                )
            elif upd_prefix[prefix]["advertisements"] > upd_stats.most_advt_prefixes[0].advertisements:
                upd_stats.most_advt_prefixes = [
                    mrt_entry(
                        prefix=prefix,
                        advertisements=upd_prefix[prefix]["advertisements"],
                    )
                ]


        for prefix in upd_prefix:
            if (upd_prefix[prefix]["withdraws"] == upd_stats.most_withd_prefixes[0].withdraws and
                upd_stats.most_withd_prefixes[0].withdraws > 0):
                upd_stats.most_withd_prefixes.append(
                    mrt_entry(
                        prefix=prefix,
                        withdraws=upd_prefix[prefix]["withdraws"],
                    )
                )
            elif upd_prefix[prefix]["withdraws"] > upd_stats.most_withd_prefixes[0].withdraws:
                upd_stats.most_withd_prefixes = [
                    mrt_entry(
                        prefix=prefix,
                        withdraws=upd_prefix[prefix]["withdraws"],
                    )
                ]

        most_updates = 0
        most_upd_prefixes = []
        for prefix in upd_prefix:
            if (upd_prefix[prefix]["advertisements"] + upd_prefix[prefix]["withdraws"]) > heighest_updates:
                heighest_updates = (upd_prefix[prefix]["advertisements"] + upd_prefix[prefix]["withdraws"])
                most_upd_prefixes = [prefix]
            elif (upd_prefix[prefix]["advertisements"] + upd_prefix[prefix]["withdraws"]) == heighest_updates:
                most_upd_prefixes.append(prefix)

        upd_stats.most_upd_prefixes = [
            mrt_entry(
                prefix=prefix,
                updates=heighest_updates,
            ) for prefix in most_upd_prefixes
        ]


        for asn in upd_peer_asn:
            if (upd_peer_asn[asn]["advertisements"] == upd_stats.most_advt_peer_asn[0].advertisements and
                upd_stats.most_advt_peer_asn[0].advertisements > 0):
                upd_stats.most_advt_peer_asn.append(
                    mrt_entry(
                        peer_asn=asn,
                        advertisements=upd_peer_asn[asn]["advertisements"],
                    )
                )
            elif upd_peer_asn[asn]["advertisements"] > upd_stats.most_advt_peer_asn[0].advertisements:
                upd_stats.most_advt_peer_asn = [
                    mrt_entry(
                        peer_asn=asn,
                        advertisements=upd_peer_asn[asn]["advertisements"],
                    )
                ]

        for asn in upd_peer_asn:
            if (upd_peer_asn[asn]["withdraws"] == upd_stats.most_withd_peer_asn[0].withdraws and
                upd_stats.most_withd_peer_asn[0].withdraws > 0):
                upd_stats.most_withd_peer_asn.append(
                    mrt_entry(
                        peer_asn=asn,
                        withdraws=upd_peer_asn[asn]["withdraws"],
                    )
                )
            elif upd_peer_asn[asn]["withdraws"] > upd_stats.most_withd_peer_asn[0].withdraws:
                upd_stats.most_withd_peer_asn = [
                    mrt_entry(
                        peer_asn=asn,
                        withdraws=upd_peer_asn[asn]["withdraws"],
                    )
                ]

        most_updates = 0
        most_upd_asns = []
        for asn in upd_peer_asn:
            if (upd_peer_asn[asn]["advertisements"] + upd_peer_asn[asn]["withdraws"]) > most_updates:
                most_updates = (upd_peer_asn[asn]["advertisements"] + upd_peer_asn[asn]["withdraws"])
                most_upd_asns = [asn]
            elif (upd_peer_asn[asn]["advertisements"] + upd_peer_asn[asn]["withdraws"]) == most_updates:
                most_upd_asns.append(asn)

        upd_stats.most_upd_peer_asn = [
            mrt_entry(
                peer_asn=asn,
                updates=most_updates,
            ) for asn in most_upd_asns
        ]

        most_origins = 0
        most_origin_prefixes = []
        for prefix in origin_asns_prefix:
            if len(origin_asns_prefix[prefix]) > most_origins:
                most_origins = len(origin_asns_prefix[prefix])
                most_origin_prefixes = [prefix]
            elif len(origin_asns_prefix[prefix]) == most_origins:
                most_origin_prefixes.append(prefix)

        upd_stats.most_origin_asns = [
            mrt_entry(
                prefix=prefix,
                origin_asns=origin_asns_prefix[prefix],
            ) for prefix in most_origin_prefixes
        ]

        advt_per_origin_asn = sorted(advt_per_origin_asn.items(), key=operator.itemgetter(1))
        upd_stats.most_advt_origin_asn = [
            mrt_entry(
                origin_asns=set(x[0]),
                advertisements=x[1],
            ) for x in advt_per_origin_asn if x[1] == advt_per_origin_asn[-1][1]
        ]


            # Is there a noticable performance hit to wrap in a "try" ?
            #else:
            #    print(f"Unknown type/subtype: {entry['type']}/{entry['subtype']}")

        toc = timeit.default_timer()
        print(f"PID {os.getpid()} completed, duration: {toc - tic}, entires: {idx + 1}")

        return upd_stats

    @staticmethod
    def test_mrt_rib_dump(filename):

        if not filename:
            raise ValueError("MRT filename missing")

        if not os.path.isfile(filename):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), filename
            )

        mrt_entries = mrtparse.Reader(filename)
        for idx, mrt_entry in enumerate(mrt_entries):
            if (mrt_entry.data["type"][0] != mrtparse.MRT_T['TABLE_DUMP_V2']):
                print(f"Entry {idx} in {filename} is not type TABLE_DUMP_V2")
                print(mrt_entry.data)
                return idx

            # RIB dumps can contain both AFIs (v4 and v6)
            if (mrt_entry.data["subtype"][0] != mrtparse.TD_V2_ST['PEER_INDEX_TABLE'] and
                mrt_entry.data["subtype"][0] != mrtparse.TD_V2_ST['RIB_IPV4_UNICAST'] and
                mrt_entry.data["subtype"][0] != mrtparse.TD_V2_ST['RIB_IPV6_UNICAST']):
                print(f"Entry {idx} in {filename} is not PEER_INDEX_TABLE or RIB_IPV4_UNICAST or RIB_IPV6_UNICAST")
                print(mrt_entry.data)
                return idx

        return idx

    @staticmethod
    def test_mrt_update_dump(data):

        if not filename:
            raise ValueError("MRT filename missing")

        if not os.path.isfile(filename):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), filename
            )

        mrt_entries = mrtparse.Reader(filename)
        for idx, mrt_entry in enumerate(mrt_entries):
            if (mrt_entry.data["type"][0] != mrtparse.MRT_T['BGP4MP_ET']):
                print(f"Entry {idx} in {filename} is not type BGP4MP_ET")
                print(mrt_entry.data)
                return idx
            
            # UPDATE dumps can contain both AFIs (v4 and v6)
            if (mrt_entry.data["subtype"][0] != mrtparse.BGP4MP_ST['BGP4MP_MESSAGE_AS4'] and
                mrt_entry.data["subtype"][0] != mrtparse.BGP4MP_ST['BGP4MP_MESSAGE']):
                print(f"Entry {idx} in {filename} is not BGP4MP_MESSAGE or BGP4MP_MESSAGE_AS4")
                print(mrt_entry.data)
                return idx

        return idx

    @staticmethod
    def mrt_count(filename):

        if not filename:
            raise ValueError("MRT filename missing")

        if not os.path.isfile(filename):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), filename
            )

        i = 0
        for entry in mrtparse.Reader(filename):
            i += 1
        return i
