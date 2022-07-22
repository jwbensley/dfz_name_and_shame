import datetime
import logging
from typing import List

from dnas.config import config as cfg
from dnas.mrt_stats import mrt_stats
from dnas.whois import whois

class report:
    """
    Class for generating human readable text reports from the stats stored in
    Redis.
    """

    @staticmethod
    def gen_txt_report(mrt_s: 'mrt_stats' = None, body: bool = True) -> List[str]:
        """
        Generate a text report using the data in an mrt stats object.
        If body == False, only generate the headline info for each stat.
        """
        if not mrt_s:
            raise ValueError(
                f"Missing required arguments: mrt_s={mrt_s}"
            )

        if type(mrt_s) != mrt_stats:
            raise TypeError(
                f"mrt_s is not an mrt stats object: {type(mrt_s)}"
            )

        if type(body) != bool:
            raise TypeError(
                f"body is not a bool: {type(body)}"
            )

        # Reduce the load on WHOIS by caching responses. It's also faster :)
        whois_cache = {}
        txt_report = []

        if mrt_s.total_upd:
            text = (
                f"For {mrt_s.ts_ymd_format()} {mrt_s.total_upd} BGP UPDATES "
                f"were parsed. {mrt_s.total_advt} UPDATES contained prefix "
                f"advertisements. {mrt_s.total_withd} UPDATES contained prefix "
                f"withdraws.\n\n"
            )

            txt_report.append(text)

        if mrt_s.bogon_origin_asns:
            text = (
                f"Prefixes with most bogon origin ASNs per prefix: "
                f"{len(mrt_s.bogon_origin_asns)} prefix(es) had "
                f"{len(mrt_s.bogon_origin_asns[0].origin_asns)} bogon origin ASNs.\n"
            )

            txt_report.append(text)

            if body:
                text = ""
                for mrt_e in mrt_s.bogon_origin_asns:
                    text += f"Prefix {mrt_e.prefix} from origin ASN(s)"
                    for asn in mrt_e.origin_asns:
                        if asn not in whois_cache:
                            whois_cache[asn] = whois.as_lookup(int(asn))
                        as_name = whois_cache[asn]
                        if as_name:
                            text += f" AS{asn} ({as_name})"
                        else:
                            text += f" AS{asn}"
                    text += "\n"
                text = text[0:-1]
                text += "\n\n"

                txt_report.append(text)

        if mrt_s.bogon_prefixes:
            text = (
                f"Bogon prefixes with most origin ASNs per prefix: "
                f"{len(mrt_s.bogon_prefixes)} bogon prefix(es) had "
                f"{len(mrt_s.bogon_prefixes[0].origin_asns)} origin ASNs.\n"
            )

            txt_report.append(text)

            if body:
                text = ""
                for mrt_e in mrt_s.bogon_prefixes:
                    text += f"Prefix {mrt_e.prefix} from origin ASN(s)"
                    for asn in mrt_e.origin_asns:
                        if asn not in whois_cache:
                            whois_cache[asn] = whois.as_lookup(int(asn))
                        as_name = whois_cache[asn]
                        if as_name:
                            text += f" AS{asn} ({as_name})"
                        else:
                            text += f" AS{asn}"
                    text += "\n"
                text = text[0:-1]
                text += "\n\n"

                txt_report.append(text)

        if mrt_s.most_bogon_asns:
            text = (
                f"ASNs originating the most bogons ASNs: "
                f"{len(mrt_s.most_bogon_asns)} ASN(s) are originating "
                f"{len(mrt_s.most_bogon_asns[0].origin_asns)} bogon ASNs.\n"
            )

            txt_report.append(text)

            if body:
                text = ""
                for mrt_e in mrt_s.most_bogon_asns:
                    asn = mrt_e.as_path[0]
                    if asn not in whois_cache:
                        whois_cache[asn] = whois.as_lookup(int(asn))
                    as_name = whois_cache[asn]
                    if as_name:
                        text += f"AS{asn} ({as_name}) "
                    else:
                        text += f"AS{asn} "
                    text += f"is originating {len(mrt_e.origin_asns)} "
                    text += f"bogon ASNs: {' '.join(list(mrt_e.origin_asns))}\n"
                text = text[0:-1]
                text += "\n\n"

                txt_report.append(text)

        if mrt_s.longest_as_path:
            text = (
                f"Longest AS path: {len(mrt_s.longest_as_path)} prefix(es) had "
                f"an AS path length of {len(mrt_s.longest_as_path[0].as_path)} "
                f"ASNs.\n"
            )

            txt_report.append(text)

            if body:

                text = ""
                for mrt_e in mrt_s.longest_as_path:
                    text += f"Prefix {mrt_e.prefix} "
                    peeras = mrt_e.peer_asn
                    if peeras not in whois_cache:
                        whois_cache[peeras] = whois.as_lookup(int(peeras))
                    if whois_cache[peeras]:
                        text += f"via peer AS{peeras} ({whois_cache[peeras]}) "
                    else:
                        text += f"via peer AS{peeras} "
                    text += f"from origin ASN(s)"
                    for asn in mrt_e.origin_asns:
                        if asn not in whois_cache:
                            whois_cache[asn] = whois.as_lookup(int(asn))
                        as_name = whois_cache[asn]
                        if as_name:
                            text += f" AS{asn} ({as_name})"
                        else:
                            text += f" AS{asn}"
                    text += f". AS Path length {len(mrt_e.as_path)}: "
                    text += f"AS{' AS'.join(mrt_e.as_path)}.\n"
                text = text[0:-1]
                text += "\n\n"
                txt_report.append(text)

        if mrt_s.longest_comm_set:
            text = (
                f"Longest community set: {len(mrt_s.longest_comm_set)} "
                f"prefix(es) had a community set length of "
                f"{len(mrt_s.longest_comm_set[0].comm_set)} communities.\n"
            )

            txt_report.append(text)

            if body:

                text = ""
                for mrt_e in mrt_s.longest_comm_set:
                    text += f"Prefix {mrt_e.prefix} "
                    peeras = mrt_e.peer_asn
                    if peeras not in whois_cache:
                        whois_cache[peeras] = whois.as_lookup(int(peeras))
                    if whois_cache[peeras]:
                        text += f"via peer AS{peeras} ({whois_cache[peeras]}) "
                    else:
                        text += f"via peer AS{peeras} "
                    text += f"from origin ASN(s)"
                    for asn in mrt_e.origin_asns:
                        if asn not in whois_cache:
                            whois_cache[asn] = whois.as_lookup(int(asn))
                        as_name = whois_cache[asn]
                        if as_name:
                            text += f" AS{asn} ({as_name})"
                        else:
                            text += f" AS{asn}"
                    text += f". Commnuity set length {(len(mrt_e.comm_set))}: "
                    text += f"{' '.join(mrt_e.comm_set)}.\n"
                text = text[0:-1]
                text += "\n\n"
                txt_report.append(text)

        if mrt_s.invalid_len:
            text = (
                f"Abnormally large/small prefixes with most origin ASNs per prefix: "
                f"{len(mrt_s.invalid_len)} large/small prefix(es) had "
                f"{len(mrt_s.invalid_len[0].origin_asns)} origin ASNs.\n"
            )

            txt_report.append(text)

            if body:
                text = ""
                for mrt_e in mrt_s.invalid_len:
                    text += f"Prefix {mrt_e.prefix} from origin ASN(s)"
                    for asn in mrt_e.origin_asns:
                        if asn not in whois_cache:
                            whois_cache[asn] = whois.as_lookup(int(asn))
                        as_name = whois_cache[asn]
                        if as_name:
                            text += f" AS{asn} ({as_name})"
                        else:
                            text += f" AS{asn}"
                    text += "\n"
                text = text[0:-1]
                text += "\n\n"

                txt_report.append(text)

        if mrt_s.most_advt_prefixes:
            text = (
                f"Most BGP advertisements per prefix: "
                f"{len(mrt_s.most_advt_prefixes)} prefix(es) had "
                f"{mrt_s.most_advt_prefixes[0].advt} advertisements.\n"
            )

            txt_report.append(text)

            if body:

                text = "Prefix(es):"
                for mrt_e in mrt_s.most_advt_prefixes:
                    text += f" {mrt_e.prefix}"
                text += "\n\n"

                txt_report.append(text)

        if mrt_s.most_upd_prefixes:
            text = (
                f"Most BGP updates per prefix: "
                f"{len(mrt_s.most_upd_prefixes)} prefix(es) had "
                f"{mrt_s.most_upd_prefixes[0].updates} updates.\n"
            )

            txt_report.append(text)

            if body:

                text = "Prefix(es):"
                for mrt_e in mrt_s.most_upd_prefixes:
                    text += f" {mrt_e.prefix}"
                text += "\n\n"

                txt_report.append(text)

        if mrt_s.most_withd_prefixes:
            text = (
                f"Most BGP withdraws per prefix: "
                f"{len(mrt_s.most_withd_prefixes)} prefix(es) had "
                f"{mrt_s.most_withd_prefixes[0].withdraws} withdraws.\n"
            )

            txt_report.append(text)

            if body:

                text = "Prefix(es):"
                for mrt_e in mrt_s.most_withd_prefixes:
                    text += f" {mrt_e.prefix}"
                text += "\n\n"

                txt_report.append(text)

        if mrt_s.most_advt_origin_asn:
            text = (
                f"Most BGP advertisements per origin ASN: "
                f"{len(mrt_s.most_advt_origin_asn)} origin ASN(s) sent "
                f"{mrt_s.most_advt_origin_asn[0].advt} advertisements.\n"
            )

            txt_report.append(text)

            if body:

                text = "Origin ASN(s):"
                for mrt_e in mrt_s.most_advt_origin_asn:
                    for asn in mrt_e.origin_asns:
                        if asn not in whois_cache:
                            whois_cache[asn] = whois.as_lookup(int(asn))
                        as_name = whois_cache[asn]
                        if as_name:
                            text += f" AS{asn} ({as_name})"
                        else:
                            text += f" AS{asn}"
                    text += ", "
                text = text[0:-2]
                text += "\n\n"

                txt_report.append(text)

        if mrt_s.most_advt_peer_asn:
            text = (
                f"Most BGP advertisements per peer ASN: "
                f"{len(mrt_s.most_advt_peer_asn)} peer ASN(s) sent "
                f"{mrt_s.most_advt_peer_asn[0].advt} advertisements.\n"
            )

            txt_report.append(text)

            if body:

                text = "Peer ASN(s):"
                for mrt_e in mrt_s.most_advt_peer_asn:
                    if mrt_e.peer_asn not in whois_cache:
                        whois_cache[mrt_e.peer_asn] = whois.as_lookup(
                            int(mrt_e.peer_asn)
                        )
                    as_name = whois_cache[mrt_e.peer_asn]
                    if as_name:
                        text += f" AS{mrt_e.peer_asn} ({as_name})"
                    else:
                        text += f" AS{mrt_e.peer_asn}"
                    text += ", "
                text = text[0:-2]
                text += "\n\n"

                txt_report.append(text)

        if mrt_s.most_upd_peer_asn:
            text = (
                f"Most BGP updates per peer ASN: "
                f"{len(mrt_s.most_upd_peer_asn)} peer ASN(s) sent "
                f"{mrt_s.most_upd_peer_asn[0].updates} updates.\n"
            )

            txt_report.append(text)

            if body:

                text = "Peer ASN(s):"
                for mrt_e in mrt_s.most_upd_peer_asn:
                    if mrt_e.peer_asn not in whois_cache:
                        whois_cache[mrt_e.peer_asn] = whois.as_lookup(
                            int(mrt_e.peer_asn)
                        )
                    as_name = whois_cache[mrt_e.peer_asn]
                    if as_name:
                        text += f" AS{mrt_e.peer_asn} ({as_name})"
                    else:
                        text += f" AS{mrt_e.peer_asn}"
                    text += ", "
                text = text[0:-2]
                text += "\n\n"

                txt_report.append(text)

        if mrt_s.most_withd_peer_asn:
            text = (
                f"Most BGP withdraws per peer ASN: "
                f"{len(mrt_s.most_withd_peer_asn)} peer ASN(s) sent "
                f"{mrt_s.most_withd_peer_asn[0].withdraws} withdraws.\n"
            )

            txt_report.append(text)

            if body:

                text = "Peer ASN(s):"
                for mrt_e in mrt_s.most_withd_peer_asn:
                    if mrt_e.peer_asn not in whois_cache:
                        whois_cache[mrt_e.peer_asn] = whois.as_lookup(
                            int(mrt_e.peer_asn)
                        )
                    as_name = whois_cache[mrt_e.peer_asn]
                    if as_name:
                        text += f" AS{mrt_e.peer_asn} ({as_name})"
                    else:
                        text += f" AS{mrt_e.peer_asn}"
                    text += ", "
                text = text[0:-2]
                text += "\n\n"

                txt_report.append(text)

        if mrt_s.most_origin_asns:
            text = (
                f"Most origin ASNs per prefix: "
                f"{len(mrt_s.most_origin_asns)} prefix(es) had "
                f"{len(mrt_s.most_origin_asns[0].origin_asns)} origin ASNs.\n"
            )

            txt_report.append(text)

            if body:

                text = ""
                for mrt_e in mrt_s.most_origin_asns:
                    text += f"Prefix {mrt_e.prefix} from origin ASN(s)"
                    for asn in mrt_e.origin_asns:
                        if asn not in whois_cache:
                            whois_cache[asn] = whois.as_lookup(int(asn))
                        as_name = whois_cache[asn]
                        if as_name:
                            text += f" AS{asn} ({as_name})"
                        else:
                            text += f" AS{asn}"
                    text += "\n"
                text = text[0:-1]
                text += "\n\n"

                txt_report.append(text)

        if mrt_s.most_unknown_attrs:
            text = (
                f"Most unknown attributes per prefix: "
                f"{len(mrt_s.most_unknown_attrs)} prefix(es) had "
                f"{len(mrt_s.most_unknown_attrs[0].unknown_attrs)} "
                "unknown attribute(s).\n"
            )

            txt_report.append(text)

            if body:

                text = ""
                for mrt_e in mrt_s.most_unknown_attrs:
                    text += f"Prefix {mrt_e.prefix} from origin ASN(s) "
                    for asn in mrt_e.origin_asns:
                        if asn not in whois_cache:
                            whois_cache[asn] = whois.as_lookup(int(asn))
                        as_name = whois_cache[asn]
                        if as_name:
                            text += f"AS{asn} ({as_name}) "
                        else:
                            text += f"AS{asn} "
                    text += f"has attribute(s) {mrt_e.unknown_attrs}\n"
                text = text[0:-1]
                text += "\n\n"

                txt_report.append(text)

        return txt_report

    @staticmethod
    def gen_txt_report_fn_ymd(ymd: str = None) -> str:
        """
        Generate and return the filename for the text file report for a
        specific date.
        """
        if not ymd:
            raise ValueError(
                f"Missing required arguments: ymd={ymd}."
            )

        if type(ymd) != str:
            raise TypeError(
                f"ymd is not a string: {type(ymd)}"
            )

        # This is just a crude check that "ymd" is in the correct format
        day = datetime.datetime.strptime(ymd, cfg.DAY_FORMAT)

        return f"{ymd}.txt"
