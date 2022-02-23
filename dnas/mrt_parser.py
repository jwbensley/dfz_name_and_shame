import errno
import os
import mrtparse
import datetime
import operator

from dnas.mrt_entry import mrt_entry
from dnas.mrt_stats import mrt_stats


class mrt_parser:
    """
    Class which provides various MRT file format parsing and testing.
    """

    @staticmethod
    def get_timestamp(filename):
        """
        Return the timestamp from the start of an MRT file.
        """
        _ = mrtparse.Reader(filename)
        d = next(_).data["timestamp"]
        ts = iter(d).__next__()
        # Use the MRT file format timestamp:
        timestamp = datetime.datetime.utcfromtimestamp(ts).strftime(
            '%Y%m%d.%H%M'
        )
        try:
            _.close()
        except StopIteration:
            pass
        return timestamp

    @staticmethod
    def parse_rib_dump(filename):
        """
        Take filename of RIB dump MRT as input and return an MRT stats obj.
        """
        rib_data = mrt_stats()
        orig_filename = '_'.join(filename.split("_")[:-1])
        mrt_entries = mrtparse.Reader(filename)

        for idx, mrt_e in enumerate(mrt_entries):
            if "prefix" not in mrt_e.data:
                continue #############################################

            origin_asns = set()
            longest_as_path = [mrt_entry()]
            longest_comm_set = [mrt_entry()]
            prefix = mrt_e.data["prefix"] + "/" + str(mrt_e.data["prefix_length"])

            for rib_entry in mrt_e.data["rib_entries"]:

                as_path = []
                origin_asn = None
                comm_set = []
                next_hop = None

                for attr in rib_entry["path_attributes"]:
                    #attr_t = path_attr["type"][0]   ##### FIX ME
                    attr_t = iter(path_attr["type"]).__next__()

                    # mrtparse.BGP_ATTR_T['AS_PATH']
                    if attr_t == 2:
                        as_path = attr["value"][0]["value"]
                        origin_asn = as_path[-1]
                        origin_asns.add(origin_asn)
                    
                    # mrtparse.BGP_ATTR_T['COMMUNITY'] or
                    # mrtparse.BGP_ATTR_T['LARGE_COMMUNITY']
                    elif (attr_t == 8 or attr_t == 32):
                        comm_set = attr["value"]

                    # mrtparse.BGP_ATTR_T['NEXT_HOP']
                    elif attr_t == 3:
                        next_hop = attr["value"]

                    # mrtparse.BGP_ATTR_T['MP_REACH_NLRI']
                    elif attr_t == 14:
                        next_hop = attr["value"]["next_hop"]

                if len(as_path) == len(longest_as_path[0].as_path):
                    known_prefixes = [mrt_e.prefix for mrt_e in longest_as_path]
                    if prefix not in known_prefixes:
                        longest_as_path.append(
                            mrt_entry(
                                prefix=prefix,
                                as_path=as_path,
                                comm_set=comm_set,
                                filename=orig_filename,
                                next_hop=next_hop,
                                origin_asns=set([origin_asn]),
                            )
                        )
                elif len(as_path) > len(longest_as_path[0].as_path):
                    longest_as_path = [
                        mrt_entry(
                            prefix=prefix,
                            as_path=as_path,
                            comm_set=comm_set,
                            filename=orig_filename,
                            next_hop=next_hop,
                            origin_asns=set([origin_asn]),
                        )
                    ]

                if len(comm_set) == len(longest_comm_set[0].comm_set):
                    known_prefixes = [mrt_e.prefix for mrt_e in longest_comm_set]
                    if prefix not in known_prefixes:
                        longest_comm_set.append(
                            mrt_entry(
                                prefix=prefix,
                                as_path=as_path,
                                comm_set=comm_set,
                                filename=orig_filename,
                                next_hop=next_hop,
                                origin_asns=set([origin_asn]),
                            )
                        )
                elif len(comm_set) > len(longest_comm_set[0].comm_set):
                    longest_comm_set = [
                        mrt_entry(
                            prefix=prefix,
                            as_path=as_path,
                            comm_set=comm_set,
                            filename=orig_filename,
                            next_hop=next_hop,
                            origin_asns=set([origin_asn]),
                        )
                    ]

            if len(longest_as_path[0].as_path) == len(rib_data.longest_as_path[0].as_path):
                rib_data.longest_as_path.extend(longest_as_path)
            elif len(longest_as_path[0].as_path) > len(rib_data.longest_as_path[0].as_path):
                rib_data.longest_as_path = longest_as_path.copy()

            if len(longest_comm_set[0].comm_set) == len(rib_data.longest_comm_set[0].comm_set):
                rib_data.longest_comm_set.extend(longest_comm_set)
            elif len(longest_comm_set[0].comm_set) > len(rib_data.longest_comm_set[0].comm_set):
                rib_data.longest_comm_set = longest_comm_set.copy()

            if len(origin_asns) == len(rib_data.most_origin_asns[0].origin_asns):
                rib_data.most_origin_asns.append(
                    mrt_entry(
                        filename = orig_filename,
                        origin_asns = origin_asns,
                        prefix = prefix,
                    )
                )
            elif len(origin_asns) > len(rib_data.most_origin_asns[0].origin_asns):
                rib_data.most_origin_asns = [
                    mrt_entry(
                        origin_asns = origin_asns,
                        filename = orig_filename,
                        prefix = prefix,
                    )
                ]

            # Is there a noticable performance hit to wrap in a "try" ?
            #else:
            #    print(f"Unknown type/subtype: {entry['type']}/{entry['subtype']}")

        return rib_data

    @staticmethod
    def parse_upd_dump(filename):
        """
        Take filename of UPDATE dump MRT as input and return an MRT stats obj.
        """
        upd_stats = mrt_stats()
        orig_filename = '_'.join(filename.split("_")[:-1])
        longest_as_path = [mrt_entry()]
        longest_comm_set = [mrt_entry()]
        origin_asns_prefix = {}
        upd_prefix = {}
        advt_per_origin_asn = {}
        upd_peer_asn = {}
        mrt_entries = mrtparse.Reader(filename)

        for idx, mrt_e in enumerate(mrt_entries):

            peer_asn = mrt_e.data["peer_as"]
            if peer_asn not in upd_peer_asn:
                upd_peer_asn[peer_asn] = {
                    "advt": 0,
                    "withdraws": 0,
                }

            timestamp = mrt_parser.get_timestamp(filename)

            as_path = []
            comm_set = []

            if len(mrt_e.data["bgp_message"]["withdrawn_routes"]) > 0:
                upd_peer_asn[peer_asn]["withdraws"] += 1

                for withdrawn_route in mrt_e.data["bgp_message"]["withdrawn_routes"]:
                    prefix = withdrawn_route["prefix"] + "/" + str(withdrawn_route["prefix_length"])
                    if prefix not in upd_prefix:
                        upd_prefix[prefix] = {
                            "advt": 0,
                            "withdraws": 1,
                        }
                        origin_asns_prefix[prefix] = set()
                    else:
                        upd_prefix[prefix]["withdraws"] += 1

            if len(mrt_e.data["bgp_message"]["path_attributes"]) > 1:
                upd_peer_asn[peer_asn]["advt"] += 1
                prefixes = []

                for path_attr in mrt_e.data["bgp_message"]["path_attributes"]:
                    #attr_t = path_attr["type"][0]   ##### FIX ME
                    attr_t = iter(path_attr["type"]).__next__()

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
                        comm_set = path_attr["value"]

                    # MP_REACH_NLRI
                    elif attr_t == 14:
                        next_hop = path_attr["value"]["next_hop"]
                        for nlri in path_attr["value"]["nlri"]:
                            prefixes.append(
                                nlri["prefix"] + "/" + str(nlri["prefix_length"])
                            )

                for nlri in mrt_e.data["bgp_message"]["nlri"]:
                    prefixes.append(nlri["prefix"] + "/" + str(nlri["prefix_length"]))

                for prefix in prefixes:

                    if prefix not in upd_prefix:
                        upd_prefix[prefix] = {
                            "advt": 1,
                            "withdraws": 0,
                        }
                        origin_asns_prefix[prefix] = set([origin_asn])
                    else:
                        upd_prefix[prefix]["advt"] += 1
                        origin_asns_prefix[prefix].add(origin_asn)


            if len(as_path) == len(longest_as_path[0].as_path):
                known_prefixes = [mrt_e.prefix for mrt_e in longest_as_path]
                for prefix in prefixes:
                    if prefix not in known_prefixes:
                        longest_as_path.append(
                            mrt_entry(
                                as_path=as_path,
                                comm_set=comm_set,
                                filename=orig_filename,
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
                        comm_set=comm_set,
                        filename=orig_filename,
                        next_hop=next_hop,
                        origin_asns=set([origin_asn]),
                        peer_asn=peer_asn,
                        prefix=prefix,
                        timestamp=timestamp,
                    ) for prefix in prefixes
                ]

            if len(comm_set) == len(longest_comm_set[0].comm_set):
                known_prefixes = [mrt_e.prefix for mrt_e in longest_comm_set]
                for prefix in prefixes:
                    if prefix not in known_prefixes:
                        longest_comm_set.append(
                            mrt_entry(
                                as_path=as_path,
                                comm_set=comm_set,
                                filename=orig_filename,
                                next_hop=next_hop,
                                origin_asns=set([origin_asn]),
                                peer_asn=peer_asn,
                                prefix=prefix,
                                timestamp=timestamp,
                            )
                        )

            elif len(comm_set) > len(longest_comm_set[0].comm_set):
                longest_comm_set = [
                    mrt_entry(
                        as_path=as_path,
                        comm_set=comm_set,
                        filename=orig_filename,
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
        if len(longest_comm_set[0].comm_set) == len(upd_stats.longest_comm_set[0].comm_set):
            s_comms = [mrt_e.comm_set for mrt_e in upd_stats.longest_comm_set]
            u_comms = [mrt_e.comm_set for mrt_e in longest_comm_set]
            for u_comm in u_comms:
                if u_comm not in s_comms:
                    upd_stats.longest_comm_set.extend(u_comm)
        elif len(longest_comm_set[0].comm_set) > len(upd_stats.longest_comm_set[0].comm_set):
            upd_stats.longest_comm_set = longest_comm_set.copy()
        """
        if len(longest_comm_set[0].comm_set) > len(upd_stats.longest_comm_set[0].comm_set):
            upd_stats.longest_comm_set = longest_comm_set.copy()


        for prefix in upd_prefix:
            if (upd_prefix[prefix]["advt"] == upd_stats.most_advt_prefixes[0].advt and
                upd_stats.most_advt_prefixes[0].advt > 0):
                upd_stats.most_advt_prefixes.append(
                    mrt_entry(
                        advt=upd_prefix[prefix]["advt"],
                        filename=orig_filename,
                        prefix=prefix,
                    )
                )
            elif upd_prefix[prefix]["advt"] > upd_stats.most_advt_prefixes[0].advt:
                upd_stats.most_advt_prefixes = [
                    mrt_entry(
                        advt=upd_prefix[prefix]["advt"],
                        filename=orig_filename,
                        prefix=prefix,
                    )
                ]


        for prefix in upd_prefix:
            if (upd_prefix[prefix]["withdraws"] == upd_stats.most_withd_prefixes[0].withdraws and
                upd_stats.most_withd_prefixes[0].withdraws > 0):
                upd_stats.most_withd_prefixes.append(
                    mrt_entry(
                        filename=orig_filename,
                        prefix=prefix,
                        withdraws=upd_prefix[prefix]["withdraws"],
                    )
                )
            elif upd_prefix[prefix]["withdraws"] > upd_stats.most_withd_prefixes[0].withdraws:
                upd_stats.most_withd_prefixes = [
                    mrt_entry(
                        filename=orig_filename,
                        prefix=prefix,
                        withdraws=upd_prefix[prefix]["withdraws"],
                    )
                ]

        most_updates = 0
        most_upd_prefixes = []
        for prefix in upd_prefix:
            if (upd_prefix[prefix]["advt"] + upd_prefix[prefix]["withdraws"]) > most_updates:
                most_updates = (upd_prefix[prefix]["advt"] + upd_prefix[prefix]["withdraws"])
                most_upd_prefixes = [prefix]
            elif (upd_prefix[prefix]["advt"] + upd_prefix[prefix]["withdraws"]) == most_updates:
                most_upd_prefixes.append(prefix)

        upd_stats.most_upd_prefixes = [
            mrt_entry(
                filename=orig_filename,
                prefix=prefix,
                updates=most_updates,
            ) for prefix in most_upd_prefixes
        ]


        for asn in upd_peer_asn:
            if (upd_peer_asn[asn]["advt"] == upd_stats.most_advt_peer_asn[0].advt and
                upd_stats.most_advt_peer_asn[0].advt > 0):
                upd_stats.most_advt_peer_asn.append(
                    mrt_entry(
                        advt=upd_peer_asn[asn]["advt"],
                        filename=orig_filename,
                        peer_asn=asn,
                    )
                )
            elif upd_peer_asn[asn]["advt"] > upd_stats.most_advt_peer_asn[0].advt:
                upd_stats.most_advt_peer_asn = [
                    mrt_entry(
                        advt=upd_peer_asn[asn]["advt"],
                        filename=orig_filename,
                        peer_asn=asn,
                    )
                ]

        for asn in upd_peer_asn:
            if (upd_peer_asn[asn]["withdraws"] == upd_stats.most_withd_peer_asn[0].withdraws and
                upd_stats.most_withd_peer_asn[0].withdraws > 0):
                upd_stats.most_withd_peer_asn.append(
                    mrt_entry(
                        filename=orig_filename,
                        peer_asn=asn,
                        withdraws=upd_peer_asn[asn]["withdraws"],
                    )
                )
            elif upd_peer_asn[asn]["withdraws"] > upd_stats.most_withd_peer_asn[0].withdraws:
                upd_stats.most_withd_peer_asn = [
                    mrt_entry(
                        filename=orig_filename,
                        peer_asn=asn,
                        withdraws=upd_peer_asn[asn]["withdraws"],
                    )
                ]

        most_updates = 0
        most_upd_asns = []
        for asn in upd_peer_asn:
            if (upd_peer_asn[asn]["advt"] + upd_peer_asn[asn]["withdraws"]) > most_updates:
                most_updates = (upd_peer_asn[asn]["advt"] + upd_peer_asn[asn]["withdraws"])
                most_upd_asns = [asn]
            elif (upd_peer_asn[asn]["advt"] + upd_peer_asn[asn]["withdraws"]) == most_updates:
                most_upd_asns.append(asn)

        upd_stats.most_upd_peer_asn = [
            mrt_entry(
                filename=orig_filename,
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
                filename=orig_filename,
                prefix=prefix,
                origin_asns=origin_asns_prefix[prefix],
            ) for prefix in most_origin_prefixes
        ]

        advt_per_origin_asn = sorted(advt_per_origin_asn.items(), key=operator.itemgetter(1))
        upd_stats.most_advt_origin_asn = [
            mrt_entry(
                filename=orig_filename,
                origin_asns=set([x[0]]),
                advt=x[1],
            ) for x in advt_per_origin_asn if x[1] == advt_per_origin_asn[-1][1]
        ]


            # Is there a noticable performance hit to wrap in a "try" ?
            #else:
            #    print(f"Unknown type/subtype: {mrt_e['type']}/{mrt_e['subtype']}")

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
        for idx, mrt_e in enumerate(mrt_entries):
            if (mrt_e.data["type"][0] != mrtparse.MRT_T['TABLE_DUMP_V2']):
                print(f"Entry {idx} in {filename} is not type TABLE_DUMP_V2")
                print(mrt_e.data)
                return idx

            # RIB dumps can contain both AFIs (v4 and v6)
            if (mrt_e.data["subtype"][0] != mrtparse.TD_V2_ST['PEER_INDEX_TABLE'] and
                mrt_e.data["subtype"][0] != mrtparse.TD_V2_ST['RIB_IPV4_UNICAST'] and
                mrt_e.data["subtype"][0] != mrtparse.TD_V2_ST['RIB_IPV6_UNICAST']):
                print(f"Entry {idx} in {filename} is not PEER_INDEX_TABLE or RIB_IPV4_UNICAST or RIB_IPV6_UNICAST")
                print(mrt_e.data)
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
        for idx, mrt_e in enumerate(mrt_entries):
            if (mrt_e.data["type"][0] != mrtparse.MRT_T['BGP4MP_ET']):
                print(f"Entry {idx} in {filename} is not type BGP4MP_ET")
                print(mrt_e.data)
                return idx
            
            # UPDATE dumps can contain both AFIs (v4 and v6)
            if (mrt_e.data["subtype"][0] != mrtparse.BGP4MP_ST['BGP4MP_MESSAGE_AS4'] and
                mrt_e.data["subtype"][0] != mrtparse.BGP4MP_ST['BGP4MP_MESSAGE']):
                print(f"Entry {idx} in {filename} is not BGP4MP_MESSAGE or BGP4MP_MESSAGE_AS4")
                print(mrt_e.data)
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
