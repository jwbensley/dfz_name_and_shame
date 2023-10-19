import datetime
import errno
import logging
import operator
import os
import traceback
import typing

import mrtparse  # type: ignore
from dnas.bogon_asn import bogon_asn
from dnas.bogon_attr import bogon_attr
from dnas.bogon_ip import bogon_ip
from dnas.config import config as cfg
from dnas.mrt_archives import mrt_archives
from dnas.mrt_entry import mrt_entry
from dnas.mrt_stats import mrt_stats


class mrt_parser:
    """
    Class which provides various MRT file format parsing and testing.
    """

    @staticmethod
    def get_timestamp(filename: str) -> str:
        """
        Return the timestamp from the start of an MRT file.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}."
            )

        if type(filename) != str:
            raise TypeError(f"filename is not a string: {type(filename)}")

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
    def posix_to_ts(posix: int) -> str:
        """
        Convert the posix timestamp in an MRT dump, to the UTC time in the
        standard format of MRTs.
        """
        if type(posix) != int:
            raise TypeError(f"posix is not a string: {type(posix)}")

        return datetime.datetime.utcfromtimestamp(posix).strftime(
            cfg.TIME_FORMAT
        )

    @staticmethod
    def parse_rib_dump(filename: str) -> "mrt_stats":
        """
        Take filename of RIB dump MRT as input and return an MRT stats obj.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}."
            )

        mrt_s = mrt_stats()

        """
        for idx, mrt_e in enumerate(mrt_entries):
            if "prefix" not in mrt_e.data:
                continue #### FIX ME - Skip the peer table record at the start?
        """

        raise NotImplementedError

        return mrt_s

    @staticmethod
    def parse_upd_dump(filename: str) -> "mrt_stats":
        """
        Take filename of UPDATE dump MRT as input and return an MRT stats obj.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}."
            )

        if type(filename) != str:
            raise TypeError(f"filename is not a string: {type(filename)}")

        """
        We will see the same data again and again, so cache "seen" data to
        speed up parsing
        """
        non_bogon_asns: dict[str, None] = {}
        bogon_origin_asns: list[mrt_entry] = []
        bogon_prefix_entries: list[mrt_entry] = []
        highest_med_prefixes: list[mrt_entry] = []
        invalid_len_entries: list[mrt_entry] = []
        longest_as_path: list[mrt_entry] = []
        longest_comm_set: list[mrt_entry] = []
        most_bogon_asns: dict[str, set] = {}
        most_unknown_attrs: list[mrt_entry] = []
        origin_asns_prefix: dict[str, set] = {}
        upd_prefix: dict[str, dict] = {}
        advt_per_origin_asn: dict[str, int] = {}
        upd_peer_asn: dict[str, dict] = {}

        # If parsing a chunk of an MRT file, try to work out the orig filename
        orig_filename = ""
        if cfg.SPLIT_DIR:
            orig_filename = "_".join(filename.split("_")[:-1])
            if not os.path.isfile(orig_filename):
                orig_filename = filename
        if not orig_filename:
            # Else, assume parsing a full MRT file
            orig_filename = filename

        file_ts = mrt_parser.get_timestamp(orig_filename)

        mrt_s = mrt_stats()
        mrt_s.timestamp = file_ts
        mrt_s.file_list.append(orig_filename)

        if cfg.SPLIT_DIR and (orig_filename != filename):
            mrt_entries = mrtparse.Reader(
                os.path.join(cfg.SPLIT_DIR, os.path.basename(filename))
            )
        else:
            mrt_entries = mrtparse.Reader(filename)

        mrt_a = mrt_archives()
        strip_comm = mrt_a.get_arch_option(orig_filename, "STRIP_COMM")

        # Sometimes the MRT files contain corrupt BGP UPDATES
        try:
            for idx, mrt_e in enumerate(mrt_entries):
                """
                Some RIPE UPDATE MRTs contain the BGP state change events,
                whereas Route-Views don't.
                Yay!
                """
                s_type = next(iter(mrt_e.data["subtype"]))
                if (
                    s_type != 1 and s_type != 4  # 1 BGP4MP_MESSAGE
                ):  # 4 BGP4MP_MESSAGE_AS4
                    continue

                """
                I'm not sure why but some MRT files contain a BGP message with
                no actual UPDATE, but they are an UPDATE, i.e. not a KEEPALIVE.
                 Yay!
                """
                if "bgp_message" not in mrt_e.data:
                    continue

                """
                Some RIPE UPDATE MRTs contain all the BGP messages types
                (OPEN, KEEPALIVE, etc), whereas Route-Views don't.
                Yay!
                """
                if (
                    next(iter(mrt_e.data["bgp_message"]["type"])) != 2
                ):  # UPDATE
                    continue
                mrt_s.total_upd += 1

                ts = mrt_parser.posix_to_ts(
                    next(iter(mrt_e.data["timestamp"].items()))[0]
                )  # E.g., 1486801684

                bogon_prefixes: list[str] = []
                comm_set: list[str] = []
                invalid_len: list[str] = []
                med = cfg.MISSING_MED
                prefixes: list[str] = []
                unknown_attrs: set[int] = set()

                peer_asn = mrt_e.data["peer_as"]
                if peer_asn not in upd_peer_asn:
                    upd_peer_asn[peer_asn] = {
                        "advt": 0,
                        "withdraws": 0,
                    }

                """
                Some RIPE MRTs don't always contain "withdraw_routes" key,
                whereas all Route-Views MRTs do.
                The key may be present, but empty. Yay!
                These are IPv4 withdraws, IPv6 withdraws are in attrib
                MP_UNREACH_NLRI
                """
                if withdrawn_routes := mrt_e.data["bgp_message"].get(
                    "withdrawn_routes"
                ):
                    upd_peer_asn[peer_asn]["withdraws"] += 1
                    mrt_s.total_withd += 1

                    for withdrawn_route in withdrawn_routes:
                        prefix = (
                            withdrawn_route["prefix"]
                            + "/"
                            + str(withdrawn_route["length"])
                        )
                        if prefix not in upd_prefix:
                            upd_prefix[prefix] = {
                                "advt": 0,
                                "withdraws": 1,
                            }
                            origin_asns_prefix[prefix] = set()
                        else:
                            upd_prefix[prefix]["withdraws"] += 1

                if path_attributes := mrt_e.data["bgp_message"].get(
                    "path_attributes"
                ):
                    upd_peer_asn[peer_asn]["advt"] += 1
                    mrt_s.total_advt += 1

                    for attr in path_attributes:
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

                        # MULTI_EXIT_DISC
                        elif attr_t == 4:
                            med = int(attr["value"])

                        # COMMUNITY or LARGE_COMMUNITY
                        elif attr_t == 8 or attr_t == 32:
                            if strip_comm:
                                comm_set.extend(
                                    [
                                        c
                                        for c in attr["value"]
                                        if strip_comm not in c
                                    ]
                                )
                            else:
                                comm_set.extend(attr["value"])

                        # MP_REACH_NLRI
                        elif attr_t == 14:
                            """
                            IPV6_UNICAST:
                            if 2 in attr["value"]["afi"] and
                            1 in attr["value"]["safi"]
                            ^ This is always the case.
                            """
                            next_hop = attr["value"]["next_hop"]
                            for nlri in attr["value"]["nlri"]:
                                prefixes.append(
                                    nlri["prefix"] + "/" + str(nlri["length"])
                                )

                        # MP_UNREACH_NLRI
                        elif attr_t == 15:
                            """
                            IPV6_UNICAST:
                            if 2 in attr["value"]["afi"] and
                            1 in attr["value"]["safi"]
                            ^ This is always the case.
                            """
                            if withdrawn_routes := attr["value"].get(
                                "withdrawn_routes"
                            ):
                                upd_peer_asn[peer_asn]["withdraws"] += 1
                                mrt_s.total_withd += 1

                            for withdrawn_route in withdrawn_routes:
                                prefix = (
                                    withdrawn_route["prefix"]
                                    + "/"
                                    + str(withdrawn_route["length"])
                                )
                                if prefix not in upd_prefix:
                                    upd_prefix[prefix] = {
                                        "advt": 0,
                                        "withdraws": 1,
                                    }
                                    origin_asns_prefix[prefix] = set()
                                else:
                                    upd_prefix[prefix]["withdraws"] += 1

                        # Unknown attribute type
                        elif bogon_attr.is_unknown(attr_t):
                            unknown_attrs.add(attr_t)

                    """
                    Note that IPv6 prefix advertisements will be encoded as an
                    NLRI attribute of a MP_REACH_NLRI update as above.
                    """
                    for prefix in prefixes:
                        if bogon_ip.is_v6_bogon(prefix):
                            bogon_prefixes.append(prefix)

                        if prefix not in upd_prefix:
                            upd_prefix[prefix] = {
                                "advt": 1,
                                "withdraws": 0,
                            }
                            origin_asns_prefix[prefix] = set([origin_asn])
                        else:
                            upd_prefix[prefix]["advt"] += 1
                            origin_asns_prefix[prefix].add(origin_asn)

                        if (
                            int(prefix.split("/")[1]) > 56
                            or int(prefix.split("/")[1]) < 16
                        ):
                            invalid_len.append(prefix)

                    """
                    IPv4 prefix advertisements are encoded in the NLRI field of
                    a BGP UPDATE message, not as a multi-protocol attribute.
                    """
                    if len(mrt_e.data["bgp_message"]["nlri"]) > 0:
                        for nlri in mrt_e.data["bgp_message"]["nlri"]:
                            prefix = nlri["prefix"] + "/" + str(nlri["length"])
                            prefixes.append(prefix)

                            if bogon_ip.is_v4_bogon(prefix):
                                bogon_prefixes.append(prefix)

                            if prefix not in upd_prefix:
                                upd_prefix[prefix] = {
                                    "advt": 1,
                                    "withdraws": 0,
                                }
                                origin_asns_prefix[prefix] = set([origin_asn])
                            else:
                                upd_prefix[prefix]["advt"] += 1
                                origin_asns_prefix[prefix].add(origin_asn)

                            if (
                                int(prefix.split("/")[1]) > 24
                                or int(prefix.split("/")[1]) < 8
                            ):
                                invalid_len.append(prefix)

                # Nothing further to do if this UPDATE was a withdraw
                if not prefixes:
                    continue

                """
                Keep unique prefixes only, with additional origin ASNs for the
                same prefix
                """
                if origin_asn not in non_bogon_asns:
                    if bogon_asn.is_bogon(int(origin_asn)):
                        for prefix in prefixes:
                            for mrt_e in bogon_origin_asns:
                                if prefix == mrt_e.prefix:
                                    if origin_asn not in mrt_e.origin_asns:
                                        mrt_e.origin_asns.add(origin_asn)
                                    break
                            else:
                                bogon_origin_asns.append(
                                    mrt_entry(
                                        as_path=as_path,
                                        comm_set=comm_set,
                                        filename=orig_filename,
                                        med=med,
                                        next_hop=next_hop,
                                        origin_asns=set([origin_asn]),
                                        peer_asn=peer_asn,
                                        prefix=prefix,
                                        timestamp=ts,
                                        unknown_attrs=unknown_attrs.copy(),
                                    )
                                )
                    else:
                        non_bogon_asns[origin_asn] = None

                """
                When the origin ASN is a bogon, find the first non-bogon ASN
                """
                if origin_asn not in non_bogon_asns:
                    i = -1
                    while bogon_asn.is_bogon(int(as_path[i])):
                        i -= 1
                        if i + len(as_path) < 0:
                            break
                    else:
                        if as_path[i] not in most_bogon_asns:
                            most_bogon_asns[as_path[i]] = set([origin_asn])
                        else:
                            most_bogon_asns[as_path[i]].add(origin_asn)

                """
                Keep unique prefixes only, with additional origin ASNs for the
                same prefix being appended to existing matching prefix
                """
                for prefix in bogon_prefixes:
                    for mrt_e in bogon_prefix_entries:
                        if prefix == mrt_e.prefix:
                            if origin_asn not in mrt_e.origin_asns:
                                mrt_e.origin_asns.add(origin_asn)
                            break
                    else:
                        bogon_prefix_entries.append(
                            mrt_entry(
                                as_path=as_path,
                                comm_set=comm_set,
                                filename=orig_filename,
                                med=med,
                                next_hop=next_hop,
                                origin_asns=set([origin_asn]),
                                peer_asn=peer_asn,
                                prefix=prefix,
                                timestamp=ts,
                                unknown_attrs=unknown_attrs.copy(),
                            )
                        )

                """
                Keep prefixes with the highest MED
                """
                if not highest_med_prefixes:
                    highest_med_prefixes = [
                        mrt_entry(
                            as_path=as_path,
                            comm_set=comm_set,
                            filename=orig_filename,
                            med=med,
                            next_hop=next_hop,
                            origin_asns=set([origin_asn]),
                            peer_asn=peer_asn,
                            prefix=prefix,
                            timestamp=ts,
                            unknown_attrs=unknown_attrs.copy(),
                        )
                        for prefix in prefixes
                    ]
                else:
                    if med == highest_med_prefixes[0].med:
                        known_prefixes = [
                            mrt_e.prefix for mrt_e in highest_med_prefixes
                        ]
                        for prefix in prefixes:
                            if prefix not in known_prefixes:
                                highest_med_prefixes.append(
                                    mrt_entry(
                                        as_path=as_path,
                                        comm_set=comm_set,
                                        filename=orig_filename,
                                        med=med,
                                        next_hop=next_hop,
                                        origin_asns=set([origin_asn]),
                                        peer_asn=peer_asn,
                                        prefix=prefix,
                                        timestamp=ts,
                                        unknown_attrs=unknown_attrs.copy(),
                                    )
                                )

                    elif med > highest_med_prefixes[0].med:
                        highest_med_prefixes = [
                            mrt_entry(
                                as_path=as_path,
                                comm_set=comm_set,
                                filename=orig_filename,
                                med=med,
                                next_hop=next_hop,
                                origin_asns=set([origin_asn]),
                                peer_asn=peer_asn,
                                prefix=prefix,
                                timestamp=ts,
                                unknown_attrs=unknown_attrs.copy(),
                            )
                            for prefix in prefixes
                        ]

                if not longest_as_path:
                    longest_as_path = [
                        mrt_entry(
                            as_path=as_path,
                            comm_set=comm_set,
                            filename=orig_filename,
                            med=med,
                            next_hop=next_hop,
                            origin_asns=set([origin_asn]),
                            peer_asn=peer_asn,
                            prefix=prefix,
                            timestamp=ts,
                            unknown_attrs=unknown_attrs.copy(),
                        )
                        for prefix in prefixes
                    ]
                else:
                    if len(as_path) == len(longest_as_path[0].as_path):
                        known_prefixes = [
                            mrt_e.prefix for mrt_e in longest_as_path
                        ]
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
                                        unknown_attrs=unknown_attrs.copy(),
                                    )
                                )

                    elif len(as_path) > len(longest_as_path[0].as_path):
                        longest_as_path = [
                            mrt_entry(
                                as_path=as_path,
                                comm_set=comm_set,
                                filename=orig_filename,
                                med=med,
                                next_hop=next_hop,
                                origin_asns=set([origin_asn]),
                                peer_asn=peer_asn,
                                prefix=prefix,
                                timestamp=ts,
                                unknown_attrs=unknown_attrs.copy(),
                            )
                            for prefix in prefixes
                        ]

                if not longest_comm_set:
                    longest_comm_set = [
                        mrt_entry(
                            as_path=as_path,
                            comm_set=comm_set,
                            filename=orig_filename,
                            med=med,
                            next_hop=next_hop,
                            origin_asns=set([origin_asn]),
                            peer_asn=peer_asn,
                            prefix=prefix,
                            timestamp=ts,
                            unknown_attrs=unknown_attrs.copy(),
                        )
                        for prefix in prefixes
                    ]
                else:
                    if len(comm_set) == len(longest_comm_set[0].comm_set):
                        known_prefixes = [
                            mrt_e.prefix for mrt_e in longest_comm_set
                        ]
                        for prefix in prefixes:
                            if prefix not in known_prefixes:
                                longest_comm_set.append(
                                    mrt_entry(
                                        as_path=as_path,
                                        comm_set=comm_set,
                                        filename=orig_filename,
                                        med=med,
                                        next_hop=next_hop,
                                        origin_asns=set([origin_asn]),
                                        peer_asn=peer_asn,
                                        prefix=prefix,
                                        timestamp=ts,
                                        unknown_attrs=unknown_attrs.copy(),
                                    )
                                )

                    elif len(comm_set) > len(longest_comm_set[0].comm_set):
                        longest_comm_set = [
                            mrt_entry(
                                as_path=as_path,
                                comm_set=comm_set,
                                filename=orig_filename,
                                med=med,
                                next_hop=next_hop,
                                origin_asns=set([origin_asn]),
                                peer_asn=peer_asn,
                                prefix=prefix,
                                timestamp=ts,
                                unknown_attrs=unknown_attrs.copy(),
                            )
                            for prefix in prefixes
                        ]

                """
                Keep unique prefixes only, with additional origin ASNs for the
                same prefix being appended to existing matching prefix:
                """
                for prefix in invalid_len:
                    for mrt_e in invalid_len_entries:
                        if prefix == mrt_e.prefix:
                            if origin_asn not in mrt_e.origin_asns:
                                mrt_e.origin_asns.add(origin_asn)
                            break
                    else:
                        invalid_len_entries.append(
                            mrt_entry(
                                as_path=as_path,
                                comm_set=comm_set,
                                filename=orig_filename,
                                med=med,
                                next_hop=next_hop,
                                origin_asns=set([origin_asn]),
                                peer_asn=peer_asn,
                                prefix=prefix,
                                timestamp=ts,
                                unknown_attrs=unknown_attrs.copy(),
                            )
                        )

                """
                Keep unique prefixes only, with additional unknown attrs for
                the same prefix being appended to existing matching prefix
                """
                if unknown_attrs:
                    for prefix in prefixes:
                        for mrt_e in most_unknown_attrs:
                            if mrt_e.prefix == prefix:
                                mrt_e.unknown_attrs.update(unknown_attrs)
                                break
                        else:
                            most_unknown_attrs.append(
                                mrt_entry(
                                    as_path=as_path,
                                    comm_set=comm_set,
                                    filename=orig_filename,
                                    next_hop=next_hop,
                                    origin_asns=set([origin_asn]),
                                    peer_asn=peer_asn,
                                    prefix=prefix,
                                    timestamp=ts,
                                    unknown_attrs=unknown_attrs.copy(),
                                )
                            )

        except KeyError as e:
            logging.error(
                f"Skipped unparsable entry in {filename} due to KeyError:\n"
                f"{traceback.format_exc()}"
            )

        # Only get the prefixes with the most bogon origin ASNs
        for mrt_e in bogon_origin_asns:
            if not mrt_s.bogon_origin_asns:
                mrt_s.bogon_origin_asns = [mrt_e]
            else:
                if (
                    len(mrt_e.origin_asns)
                    == len(mrt_s.bogon_origin_asns[0].origin_asns)
                    and mrt_e.origin_asns
                ):
                    mrt_s.bogon_origin_asns.append(mrt_e)
                elif len(mrt_e.origin_asns) > len(
                    mrt_s.bogon_origin_asns[0].origin_asns
                ):
                    mrt_s.bogon_origin_asns = [mrt_e]

        # Only get the bogons prefixes with the most origin ASNs
        for mrt_e in bogon_prefix_entries:
            if not mrt_s.bogon_prefixes:
                mrt_s.bogon_prefixes = [mrt_e]
            else:
                if (
                    len(mrt_e.origin_asns)
                    == len(mrt_s.bogon_prefixes[0].origin_asns)
                    and mrt_e.origin_asns
                ):
                    mrt_s.bogon_prefixes.append(mrt_e)
                elif len(mrt_e.origin_asns) > len(
                    mrt_s.bogon_prefixes[0].origin_asns
                ):
                    mrt_s.bogon_prefixes = [mrt_e]

        mrt_s.highest_med_prefixes = highest_med_prefixes.copy()

        # Only get the invalid mask lengths with the most origin ASNs
        for mrt_e in invalid_len_entries:
            if not mrt_s.invalid_len:
                mrt_s.invalid_len = [mrt_e]
            else:
                if (
                    len(mrt_e.origin_asns)
                    == len(mrt_s.invalid_len[0].origin_asns)
                    and mrt_e.origin_asns
                ):
                    mrt_s.invalid_len.append(mrt_e)
                elif len(mrt_e.origin_asns) > len(
                    mrt_s.invalid_len[0].origin_asns
                ):
                    mrt_s.invalid_len = [mrt_e]

        mrt_s.longest_as_path = longest_as_path.copy()

        mrt_s.longest_comm_set = longest_comm_set.copy()

        # Only get the ASNs originating the most bogon downstream ASNs
        bogon_asn_count = 0
        for asn in most_bogon_asns:
            if len(most_bogon_asns[asn]) > bogon_asn_count:
                bogon_asn_count = len(most_bogon_asns[asn])
                mrt_s.most_bogon_asns = [
                    mrt_entry(
                        as_path=[asn],
                        origin_asns=most_bogon_asns[asn],
                        filename=orig_filename,
                        timestamp=file_ts,
                    )
                ]
            elif len(most_bogon_asns[asn]) == bogon_asn_count:
                mrt_s.most_bogon_asns.append(
                    mrt_entry(
                        as_path=[asn],
                        origin_asns=most_bogon_asns[asn],
                        filename=orig_filename,
                        timestamp=file_ts,
                    )
                )

        most_updates = 0
        most_upd_prefixes = []
        for prefix in upd_prefix:
            if not mrt_s.most_advt_prefixes:
                mrt_s.most_advt_prefixes = [
                    mrt_entry(
                        advt=upd_prefix[prefix]["advt"],
                        filename=orig_filename,
                        prefix=prefix,
                        timestamp=file_ts,
                    )
                ]
            else:
                if (
                    upd_prefix[prefix]["advt"]
                    == mrt_s.most_advt_prefixes[0].advt
                    and mrt_s.most_advt_prefixes[0].advt > 0
                ):
                    mrt_s.most_advt_prefixes.append(
                        mrt_entry(
                            advt=upd_prefix[prefix]["advt"],
                            filename=orig_filename,
                            prefix=prefix,
                            timestamp=file_ts,
                        )
                    )
                elif (
                    upd_prefix[prefix]["advt"]
                    > mrt_s.most_advt_prefixes[0].advt
                ):
                    mrt_s.most_advt_prefixes = [
                        mrt_entry(
                            advt=upd_prefix[prefix]["advt"],
                            filename=orig_filename,
                            prefix=prefix,
                            timestamp=file_ts,
                        )
                    ]

            if not mrt_s.most_withd_prefixes:
                mrt_s.most_withd_prefixes = [
                    mrt_entry(
                        filename=orig_filename,
                        prefix=prefix,
                        timestamp=file_ts,
                        withdraws=upd_prefix[prefix]["withdraws"],
                    )
                ]
            else:
                if (
                    upd_prefix[prefix]["withdraws"]
                    == mrt_s.most_withd_prefixes[0].withdraws
                    and mrt_s.most_withd_prefixes[0].withdraws > 0
                ):
                    mrt_s.most_withd_prefixes.append(
                        mrt_entry(
                            filename=orig_filename,
                            prefix=prefix,
                            timestamp=file_ts,
                            withdraws=upd_prefix[prefix]["withdraws"],
                        )
                    )
                elif (
                    upd_prefix[prefix]["withdraws"]
                    > mrt_s.most_withd_prefixes[0].withdraws
                ):
                    mrt_s.most_withd_prefixes = [
                        mrt_entry(
                            filename=orig_filename,
                            prefix=prefix,
                            timestamp=file_ts,
                            withdraws=upd_prefix[prefix]["withdraws"],
                        )
                    ]

            if (
                upd_prefix[prefix]["advt"] + upd_prefix[prefix]["withdraws"]
            ) > most_updates:
                most_updates = (
                    upd_prefix[prefix]["advt"]
                    + upd_prefix[prefix]["withdraws"]
                )
                most_upd_prefixes = [prefix]
            elif (
                upd_prefix[prefix]["advt"] + upd_prefix[prefix]["withdraws"]
            ) == most_updates:
                most_upd_prefixes.append(prefix)

        mrt_s.most_upd_prefixes = [
            mrt_entry(
                filename=orig_filename,
                prefix=prefix,
                timestamp=file_ts,
                updates=most_updates,
            )
            for prefix in most_upd_prefixes
        ]

        most_updates = 0
        most_upd_asns = []
        for asn in upd_peer_asn:
            if not mrt_s.most_advt_peer_asn:
                mrt_s.most_advt_peer_asn = [
                    mrt_entry(
                        advt=upd_peer_asn[asn]["advt"],
                        filename=orig_filename,
                        peer_asn=asn,
                        timestamp=file_ts,
                    )
                ]
            else:
                if (
                    upd_peer_asn[asn]["advt"]
                    == mrt_s.most_advt_peer_asn[0].advt
                    and mrt_s.most_advt_peer_asn[0].advt > 0
                ):
                    mrt_s.most_advt_peer_asn.append(
                        mrt_entry(
                            advt=upd_peer_asn[asn]["advt"],
                            filename=orig_filename,
                            peer_asn=asn,
                            timestamp=file_ts,
                        )
                    )
                elif (
                    upd_peer_asn[asn]["advt"]
                    > mrt_s.most_advt_peer_asn[0].advt
                ):
                    mrt_s.most_advt_peer_asn = [
                        mrt_entry(
                            advt=upd_peer_asn[asn]["advt"],
                            filename=orig_filename,
                            peer_asn=asn,
                            timestamp=file_ts,
                        )
                    ]

            if not mrt_s.most_withd_peer_asn:
                mrt_s.most_withd_peer_asn = [
                    mrt_entry(
                        filename=orig_filename,
                        peer_asn=asn,
                        timestamp=file_ts,
                        withdraws=upd_peer_asn[asn]["withdraws"],
                    )
                ]
            else:
                if (
                    upd_peer_asn[asn]["withdraws"]
                    == mrt_s.most_withd_peer_asn[0].withdraws
                    and mrt_s.most_withd_peer_asn[0].withdraws > 0
                ):
                    mrt_s.most_withd_peer_asn.append(
                        mrt_entry(
                            filename=orig_filename,
                            peer_asn=asn,
                            timestamp=file_ts,
                            withdraws=upd_peer_asn[asn]["withdraws"],
                        )
                    )
                elif (
                    upd_peer_asn[asn]["withdraws"]
                    > mrt_s.most_withd_peer_asn[0].withdraws
                ):
                    mrt_s.most_withd_peer_asn = [
                        mrt_entry(
                            filename=orig_filename,
                            peer_asn=asn,
                            timestamp=file_ts,
                            withdraws=upd_peer_asn[asn]["withdraws"],
                        )
                    ]

            if (
                upd_peer_asn[asn]["advt"] + upd_peer_asn[asn]["withdraws"]
            ) > most_updates:
                most_updates = (
                    upd_peer_asn[asn]["advt"] + upd_peer_asn[asn]["withdraws"]
                )
                most_upd_asns = [asn]
            elif (
                upd_peer_asn[asn]["advt"] + upd_peer_asn[asn]["withdraws"]
            ) == most_updates:
                most_upd_asns.append(asn)

        mrt_s.most_upd_peer_asn = [
            mrt_entry(
                filename=orig_filename,
                peer_asn=asn,
                timestamp=file_ts,
                updates=most_updates,
            )
            for asn in most_upd_asns
        ]

        for prefix in origin_asns_prefix:
            if not mrt_s.most_origin_asns:
                mrt_s.most_origin_asns.append(
                    mrt_entry(
                        filename=orig_filename,
                        origin_asns=origin_asns_prefix[prefix],
                        prefix=prefix,
                        timestamp=file_ts,
                    )
                )
            else:
                if len(origin_asns_prefix[prefix]) > len(
                    mrt_s.most_origin_asns[0].origin_asns
                ):
                    mrt_s.most_origin_asns = [
                        mrt_entry(
                            filename=orig_filename,
                            origin_asns=origin_asns_prefix[prefix],
                            prefix=prefix,
                            timestamp=file_ts,
                        )
                    ]
                elif len(origin_asns_prefix[prefix]) == len(
                    mrt_s.most_origin_asns[0].origin_asns
                ):
                    mrt_s.most_origin_asns.append(
                        mrt_entry(
                            filename=orig_filename,
                            origin_asns=origin_asns_prefix[prefix],
                            prefix=prefix,
                            timestamp=file_ts,
                        )
                    )

        advt_per_orig_asn_sorted = sorted(
            advt_per_origin_asn.items(), key=operator.itemgetter(1)
        )
        mrt_s.most_advt_origin_asn = [
            mrt_entry(
                advt=x[1],
                filename=orig_filename,
                origin_asns=set([x[0]]),
                timestamp=file_ts,
            )
            for x in advt_per_orig_asn_sorted
            if x[1] == advt_per_orig_asn_sorted[-1][1]
        ]

        # Only get the prefixes with the most unknown attributes
        for mrt_e in most_unknown_attrs:
            if not mrt_s.most_unknown_attrs:
                mrt_s.most_unknown_attrs = [mrt_e]
            else:
                if (
                    len(mrt_e.unknown_attrs)
                    == len(mrt_s.most_unknown_attrs[0].unknown_attrs)
                    and mrt_e.unknown_attrs
                ):
                    mrt_s.most_unknown_attrs.append(mrt_e)
                elif len(mrt_e.unknown_attrs) > len(
                    mrt_s.most_unknown_attrs[0].unknown_attrs
                ):
                    mrt_s.most_unknown_attrs = [mrt_e]

        return mrt_s

    @staticmethod
    def mrt_count(filename: str) -> int:
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
