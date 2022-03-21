import datetime
import errno
import logging
import mrtparse # type: ignore
import operator
import os
from typing import Dict

from dnas.config import config as cfg
from dnas.mrt_entry import mrt_entry
from dnas.mrt_stats import mrt_stats


class mrt_parser:
    """
    Class which provides various MRT file format parsing and testing.
    """

    @staticmethod
    def get_timestamp(filename: str = None) -> str:
        """
        Return the timestamp from the start of an MRT file.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}."
            )

        if type(filename) != str:
            raise TypeError(
                f"filename is not a string: {type(filename)}"
            )

        _ = mrtparse.Reader(filename)
        d = next(_).data["timestamp"]
        ts = next(iter(d))
        # Use the MRT file format timestamp:
        timestamp = datetime.datetime.utcfromtimestamp(ts).strftime(
            cfg.TIME_FORMAT
        )
        try:
            _.close()
        except StopIteration:
            pass
        return timestamp

    @staticmethod
    def posix_to_ts(posix: int = None) -> str:
        """
        Convert the posix timestamp in an MRT dump, to the UTC time in the
        standard format of MRTs.
        """
        if not posix:
            raise ValueError(
                f"Missing required arguments: posix={posix}."
            )

        if type(posix) != int:
            raise TypeError(
                f"posix is not a string: {type(posix)}"
            )

        return datetime.datetime.utcfromtimestamp(posix).strftime(
            cfg.TIME_FORMAT
        )

    @staticmethod
    def parse_rib_dump(filename: str = None) -> 'mrt_stats':
        """
        Take filename of RIB dump MRT as input and return an MRT stats obj.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}."
            )

        # If parsing a chunk of an MRT file, try to work out the orig filename
        if cfg.SPLIT_DIR:
            orig_filename = '_'.join(filename.split("_")[:-1])
        else:
            # Else, assume parsing a full MRT file
            orig_filename = filename
        file_ts = mrt_parser.get_timestamp(orig_filename)

        mrt_s = mrt_stats()
        mrt_s.timestamp = file_ts
        mrt_s.file_list.append(orig_filename)

        if cfg.SPLIT_DIR:
            mrt_entries = mrtparse.Reader(
                os.path.join(cfg.SPLIT_DIR, os.path.basename(filename))
            )
        else:
            mrt_entries = mrtparse.Reader(filename)

        for idx, mrt_e in enumerate(mrt_entries):
            if "prefix" not in mrt_e.data:
                continue #### FIX ME - Skip the peer table record at the start?

            ts = mrt_parser.posix_to_ts(
                next(iter(mrt_e.data["timestamp"].items()))[0]
            ) # E.g., 1486801684
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
                    #attr_t = attr["type"][0]   ##### FIX ME
                    attr_t = next(iter(attr["type"]))

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
                                timestamp=ts,
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
                            timestamp=ts,
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
                                timestamp=ts,
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
                            timestamp=ts,
                        )
                    ]

            if len(longest_as_path[0].as_path) == len(mrt_s.longest_as_path[0].as_path):
                mrt_s.longest_as_path.extend(longest_as_path)
            elif len(longest_as_path[0].as_path) > len(mrt_s.longest_as_path[0].as_path):
                mrt_s.longest_as_path = longest_as_path.copy()

            if len(longest_comm_set[0].comm_set) == len(mrt_s.longest_comm_set[0].comm_set):
                mrt_s.longest_comm_set.extend(longest_comm_set)
            elif len(longest_comm_set[0].comm_set) > len(mrt_s.longest_comm_set[0].comm_set):
                mrt_s.longest_comm_set = longest_comm_set.copy()

            if len(origin_asns) == len(mrt_s.most_origin_asns[0].origin_asns):
                mrt_s.most_origin_asns.append(
                    mrt_entry(
                        filename = orig_filename,
                        origin_asns = origin_asns,
                        prefix = prefix,
                        timestamp=ts,
                    )
                )
            elif len(origin_asns) > len(mrt_s.most_origin_asns[0].origin_asns):
                mrt_s.most_origin_asns = [
                    mrt_entry(
                        origin_asns = origin_asns,
                        filename = orig_filename,
                        prefix = prefix,
                        timestamp=ts,
                    )
                ]

        return mrt_s

    @staticmethod
    def parse_upd_dump(filename: str = None) -> 'mrt_stats':
        """
        Take filename of UPDATE dump MRT as input and return an MRT stats obj.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}."
            )

        longest_as_path = [mrt_entry()]
        longest_comm_set = [mrt_entry()]
        origin_asns_prefix: Dict[str, set] = {}
        upd_prefix: Dict[str, dict] = {}
        advt_per_origin_asn: Dict[str, int] = {}
        upd_peer_asn: Dict[str, dict] = {}

        # If parsing a chunk of an MRT file, try to work out the orig filename
        if cfg.SPLIT_DIR:
            orig_filename = '_'.join(filename.split("_")[:-1])
        else:
            # Else, assume parsing a full MRT file
            orig_filename = filename

        file_ts = mrt_parser.get_timestamp(orig_filename)

        mrt_s = mrt_stats()
        mrt_s.timestamp = file_ts
        mrt_s.file_list.append(orig_filename)

        if cfg.SPLIT_DIR:
            mrt_entries = mrtparse.Reader(
                os.path.join(cfg.SPLIT_DIR, os.path.basename(filename))
            )
        else:
            mrt_entries = mrtparse.Reader(filename)

        for idx, mrt_e in enumerate(mrt_entries):

            """
            Some RIPE UPDATE MRTs contain the BGP state change events, whereas
            Route-Views don't. Yay!
            """
            s_type = next(iter(mrt_e.data["subtype"]))
            if (s_type != 1 and # 1 BGP4MP_MESSAGE
                s_type != 4): # 4 BGP4MP_MESSAGE_AS4
                continue

            """
            Some RIPE UPDATE MRTs contain all the BGP messages types
            (OPEN, KEEPALIVE, etc), whereas Route-Views don't. Yay!
            """
            if next(iter(mrt_e.data["bgp_message"]["type"])) != 2: # UPDATE
                continue

            ts = mrt_parser.posix_to_ts(
                next(iter(mrt_e.data["timestamp"].items()))[0]
            ) # E.g., 1486801684

            as_path = []
            comm_set = []

            peer_asn = mrt_e.data["peer_as"]
            if peer_asn not in upd_peer_asn:
                upd_peer_asn[peer_asn] = {
                    "advt": 0,
                    "withdraws": 0,
                }

            """
            Some RIPE MRTs don't always contain "withdraw_routes" key, whereas
            all Route-Views MRTs do. Yay!
            """
            if "withdrawn_routes" in ["bgp_message"]:
                if len(mrt_e.data["bgp_message"]) > 0:
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

                for attr in mrt_e.data["bgp_message"]["path_attributes"]:
                    #attr_t = attr["type"][0]   ##### FIX ME
                    attr_t = next(iter(attr["type"]))

                    # AS_PATH
                    if attr_t == 2:
                        as_path = attr["value"][0]["value"]
                        origin_asn = as_path[-1]
                        if origin_asn not in advt_per_origin_asn:
                            advt_per_origin_asn[origin_asn] = 1
                        else:
                            advt_per_origin_asn[origin_asn] += 1

                    # NEXT_HOP
                    elif attr_t == 3:
                        next_hop = attr["value"]

                    # COMMUNITY or LARGE_COMMUNITY
                    elif (attr_t == 8 or attr_t == 32):
                        comm_set = attr["value"]

                    # MP_REACH_NLRI
                    elif attr_t == 14:
                        next_hop = attr["value"]["next_hop"]
                        for nlri in attr["value"]["nlri"]:
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
                                timestamp=ts,
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
                        timestamp=ts,
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
                                timestamp=ts,
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
                        timestamp=ts,
                    ) for prefix in prefixes
                ]


        ################## FIX ME - REMOVE "if" mrt_s is empty
        if len(longest_as_path[0].as_path) > len(mrt_s.longest_as_path[0].as_path):
            mrt_s.longest_as_path = longest_as_path.copy()

        ################## FIX ME - REMOVE "if" mrt_s is empty
        if len(longest_comm_set[0].comm_set) > len(mrt_s.longest_comm_set[0].comm_set):
            mrt_s.longest_comm_set = longest_comm_set.copy()

        for prefix in upd_prefix:
            if (upd_prefix[prefix]["advt"] == mrt_s.most_advt_prefixes[0].advt and
                mrt_s.most_advt_prefixes[0].advt > 0):
                mrt_s.most_advt_prefixes.append(
                    mrt_entry(
                        advt=upd_prefix[prefix]["advt"],
                        filename=orig_filename,
                        prefix=prefix,
                        timestamp=file_ts,

                    )
                )
            elif upd_prefix[prefix]["advt"] > mrt_s.most_advt_prefixes[0].advt:
                mrt_s.most_advt_prefixes = [
                    mrt_entry(
                        advt=upd_prefix[prefix]["advt"],
                        filename=orig_filename,
                        prefix=prefix,
                        timestamp=file_ts,
                    )
                ]


        for prefix in upd_prefix:
            if (upd_prefix[prefix]["withdraws"] == mrt_s.most_withd_prefixes[0].withdraws and
                mrt_s.most_withd_prefixes[0].withdraws > 0):
                mrt_s.most_withd_prefixes.append(
                    mrt_entry(
                        filename=orig_filename,
                        prefix=prefix,
                        timestamp=file_ts,
                        withdraws=upd_prefix[prefix]["withdraws"],
                    )
                )
            elif upd_prefix[prefix]["withdraws"] > mrt_s.most_withd_prefixes[0].withdraws:
                mrt_s.most_withd_prefixes = [
                    mrt_entry(
                        filename=orig_filename,
                        prefix=prefix,
                        timestamp=file_ts,
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

        mrt_s.most_upd_prefixes = [
            mrt_entry(
                filename=orig_filename,
                prefix=prefix,
                timestamp=file_ts,
                updates=most_updates,
            ) for prefix in most_upd_prefixes
        ]


        for asn in upd_peer_asn:
            if (upd_peer_asn[asn]["advt"] == mrt_s.most_advt_peer_asn[0].advt and
                mrt_s.most_advt_peer_asn[0].advt > 0):
                mrt_s.most_advt_peer_asn.append(
                    mrt_entry(
                        advt=upd_peer_asn[asn]["advt"],
                        filename=orig_filename,
                        peer_asn=asn,
                        timestamp=file_ts,
                    )
                )
            elif upd_peer_asn[asn]["advt"] > mrt_s.most_advt_peer_asn[0].advt:
                mrt_s.most_advt_peer_asn = [
                    mrt_entry(
                        advt=upd_peer_asn[asn]["advt"],
                        filename=orig_filename,
                        peer_asn=asn,
                        timestamp=file_ts,
                    )
                ]

        for asn in upd_peer_asn:
            if (upd_peer_asn[asn]["withdraws"] == mrt_s.most_withd_peer_asn[0].withdraws and
                mrt_s.most_withd_peer_asn[0].withdraws > 0):
                mrt_s.most_withd_peer_asn.append(
                    mrt_entry(
                        filename=orig_filename,
                        peer_asn=asn,
                        timestamp=file_ts,
                        withdraws=upd_peer_asn[asn]["withdraws"],
                    )
                )
            elif upd_peer_asn[asn]["withdraws"] > mrt_s.most_withd_peer_asn[0].withdraws:
                mrt_s.most_withd_peer_asn = [
                    mrt_entry(
                        filename=orig_filename,
                        peer_asn=asn,
                        timestamp=file_ts,
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

        mrt_s.most_upd_peer_asn = [
            mrt_entry(
                filename=orig_filename,
                peer_asn=asn,
                timestamp=file_ts,
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

        mrt_s.most_origin_asns = [
            mrt_entry(
                filename=orig_filename,
                origin_asns=origin_asns_prefix[prefix],
                prefix=prefix,
                timestamp=file_ts,
            ) for prefix in most_origin_prefixes
        ]

        advt_per_orig_asn_sorted = sorted(advt_per_origin_asn.items(), key=operator.itemgetter(1))
        mrt_s.most_advt_origin_asn = [
            mrt_entry(
                advt=x[1],
                filename=orig_filename,
                origin_asns=set([x[0]]),
                timestamp=file_ts,
            ) for x in advt_per_orig_asn_sorted if x[1] == advt_per_orig_asn_sorted[-1][1]
        ]

        return mrt_s

    @staticmethod
    def test_mrt_rib_dump(filename: str = None) -> int:

        if not filename:
            raise ValueError("MRT filename missing")

        if not os.path.isfile(filename):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), filename
            )

        mrt_entries = mrtparse.Reader(filename)
        for idx, mrt_e in enumerate(mrt_entries):
            if (mrt_e.data["type"][0] != mrtparse.MRT_T['TABLE_DUMP_V2']):
                logging.error(
                    f"Entry {idx} in {filename} is not type TABLE_DUMP_V2"
                )
                logging.error(mrt_e.data)
                return idx

            # RIB dumps can contain both AFIs (v4 and v6)
            if (mrt_e.data["subtype"][0] != mrtparse.TD_V2_ST['PEER_INDEX_TABLE'] and
                mrt_e.data["subtype"][0] != mrtparse.TD_V2_ST['RIB_IPV4_UNICAST'] and
                mrt_e.data["subtype"][0] != mrtparse.TD_V2_ST['RIB_IPV6_UNICAST']):
                logging.error(
                    f"Entry {idx} in {filename} is not PEER_INDEX_TABLE or "
                    f"RIB_IPV4_UNICAST or RIB_IPV6_UNICAST"
                )
                logging.error(mrt_e.data)
                return idx

        return idx

    @staticmethod
    def test_mrt_update_dump(filename: str = None) -> int:

        if not filename:
            raise ValueError("MRT filename missing")

        if not os.path.isfile(filename):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), filename
            )

        mrt_entries = mrtparse.Reader(filename)
        for idx, mrt_e in enumerate(mrt_entries):
            if (mrt_e.data["type"][0] != mrtparse.MRT_T['BGP4MP_ET']):
                logging.error(
                    f"Entry {idx} in {filename} is not type BGP4MP_ET"
                )
                logging.error(mrt_e.data)
                return idx
            
            # UPDATE dumps can contain both AFIs (v4 and v6)
            if (mrt_e.data["subtype"][0] != mrtparse.BGP4MP_ST['BGP4MP_MESSAGE_AS4'] and
                mrt_e.data["subtype"][0] != mrtparse.BGP4MP_ST['BGP4MP_MESSAGE']):
                logging.error(
                    f"Entry {idx} in {filename} is not BGP4MP_MESSAGE or "
                    f"BGP4MP_MESSAGE_AS4"
                )
                logging.error(mrt_e.data)
                return idx

        return idx

    @staticmethod
    def mrt_count(filename: str = None) -> int:
        """
        Return the total number of MRT records in an MRT file.
        """
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
