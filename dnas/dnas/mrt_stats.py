import datetime
import json

from dnas.config import config as cfg
from dnas.mrt_archive import mrt_archive
from dnas.mrt_entry import mrt_entry


class mrt_stats:
    """
    An MRT Stats object contains lists of MRT Entry objects, plus some metadata.
    This stores the stats from a parsed data source (i.e. a BGP MRT dump).
    """

    def __init__(self: "mrt_stats") -> None:
        self.archive_list: set[str] = (
            set()
        )  # Archives from which this stats object was populated
        self.bogon_origin_asns: list[mrt_entry] = []
        self.bogon_prefixes: list[mrt_entry] = []
        self.highest_med_prefixes: list[mrt_entry] = []
        self.invalid_len: list[mrt_entry] = []
        self.longest_as_path: list[mrt_entry] = []
        self.longest_comm_set: list[mrt_entry] = []
        self.most_advt_prefixes: list[mrt_entry] = []
        self.most_bogon_asns: list[mrt_entry] = []
        self.most_upd_prefixes: list[mrt_entry] = []
        self.most_withd_prefixes: list[mrt_entry] = []
        self.most_advt_origin_asn: list[mrt_entry] = []
        self.most_advt_peer_asn: list[mrt_entry] = []
        self.most_upd_peer_asn: list[mrt_entry] = []
        self.most_withd_peer_asn: list[mrt_entry] = []
        self.most_origin_asns: list[mrt_entry] = []
        self.most_unknown_attrs: list[mrt_entry] = []
        self.most_unreg_origins: list[mrt_entry] = []
        self.file_list: list[str] = []
        self.timestamp: str = ""
        self.total_upd: int = 0  # All updates received/parsed
        self.total_advt: int = 0  # Updates signalling prefix advertisement
        self.total_withd: int = 0  # Updates signalling prefix withdrawal

    def add(self: "mrt_stats", merge_data: "mrt_stats") -> bool:
        """
        This function adds another MRT stats object into this one.
        This means that values which are equal in both objects are added and
        the result is stored in this obj. If merge_data has a different value
        which is higher, the smaller value in this object is replaced with the
        higher value.

        E.g, if both objects have the same "max updates per prefix" prefix,
        192.168.0.0/24, with both objects recording 1000 updates for this
        prefix, this obj will now record 192.168.0.0/24 as having 2000
        updates. But, if merge_data has a prefix, 192.168.1.0/24, with 10000
        updates, 192.168.1.0/24 will replace 192.168.0.0/24 in this object.
        """
        if not merge_data:
            raise ValueError(
                f"Missing required options: merge_data={merge_data}"
            )

        if type(merge_data) != mrt_stats:
            raise TypeError(
                f"merge_data is not a stats object: {type(merge_data)}"
            )

        changed = False

        # Prefixes with most bogon origin ASN
        tmp: list[mrt_entry] = []
        for u_e in merge_data.bogon_origin_asns[:]:
            for res_e in self.bogon_origin_asns:
                if (
                    res_e.prefix == u_e.prefix
                    and res_e.origin_asns != u_e.origin_asns
                ):
                    tmp.append(
                        mrt_entry(
                            filename=u_e.filename,
                            origin_asns=res_e.origin_asns.union(
                                u_e.origin_asns
                            ),
                            prefix=res_e.prefix,
                            timestamp=u_e.timestamp,
                        )
                    )

        if tmp:
            for tmp_e in tmp:
                if len(tmp_e.origin_asns) == len(
                    self.bogon_origin_asns[0].origin_asns
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.bogon_origin_asns
                    ]
                    if tmp_e.prefix not in s_prefixes:
                        self.bogon_origin_asns.append(tmp_e)
                        changed = True
                elif len(tmp_e.origin_asns) > len(
                    self.bogon_origin_asns[0].origin_asns
                ):
                    self.bogon_origin_asns = [tmp_e]
                    changed = True
        else:
            if merge_data.bogon_origin_asns:
                if self.bogon_origin_asns:
                    if (
                        len(merge_data.bogon_origin_asns[0].origin_asns)
                        == len(self.bogon_origin_asns[0].origin_asns)
                        and len(self.bogon_origin_asns[0].origin_asns) > 0
                    ):
                        s_prefixes = [
                            mrt_e.prefix for mrt_e in self.bogon_origin_asns
                        ]
                        for mrt_e in merge_data.bogon_origin_asns:
                            if mrt_e.prefix not in s_prefixes:
                                self.bogon_origin_asns.append(mrt_e)
                                changed = True
                    elif len(
                        merge_data.bogon_origin_asns[0].origin_asns
                    ) > len(self.bogon_origin_asns[0].origin_asns):
                        self.bogon_origin_asns = (
                            merge_data.bogon_origin_asns.copy()
                        )
                        changed = True
                else:
                    self.bogon_origin_asns = (
                        merge_data.bogon_origin_asns.copy()
                    )
                    changed = True

        # Bogons prefixes with most origin ASNs
        tmp = []
        for u_e in merge_data.bogon_prefixes[:]:
            for res_e in self.bogon_prefixes:
                if (
                    res_e.prefix == u_e.prefix
                    and res_e.origin_asns != u_e.origin_asns
                ):
                    tmp.append(
                        mrt_entry(
                            filename=u_e.filename,
                            origin_asns=res_e.origin_asns.union(
                                u_e.origin_asns
                            ),
                            prefix=res_e.prefix,
                            timestamp=u_e.timestamp,
                        )
                    )

        if tmp:
            for tmp_e in tmp:
                if len(tmp_e.origin_asns) == len(
                    self.bogon_prefixes[0].origin_asns
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.bogon_prefixes
                    ]
                    if tmp_e.prefix not in s_prefixes:
                        self.bogon_prefixes.append(tmp_e)
                        changed = True
                elif len(tmp_e.origin_asns) > len(
                    self.bogon_prefixes[0].origin_asns
                ):
                    self.bogon_prefixes = [tmp_e]
                    changed = True
        else:
            if merge_data.bogon_prefixes:
                if self.bogon_prefixes:
                    if (
                        len(merge_data.bogon_prefixes[0].origin_asns)
                        == len(self.bogon_prefixes[0].origin_asns)
                        and len(self.bogon_prefixes[0].origin_asns) > 0
                    ):
                        s_prefixes = [
                            mrt_e.prefix for mrt_e in self.bogon_prefixes
                        ]
                        for mrt_e in merge_data.bogon_prefixes:
                            if mrt_e.prefix not in s_prefixes:
                                self.bogon_prefixes.append(mrt_e)
                                changed = True
                    elif len(merge_data.bogon_prefixes[0].origin_asns) > len(
                        self.bogon_prefixes[0].origin_asns
                    ):
                        self.bogon_prefixes = merge_data.bogon_prefixes.copy()
                        changed = True
                else:
                    self.bogon_prefixes = merge_data.bogon_prefixes.copy()
                    changed = True

        # Prefixes with most unregistered origin ASN
        tmp = []
        for u_e in merge_data.most_unreg_origins[:]:
            for res_e in self.most_unreg_origins:
                if (
                    res_e.prefix == u_e.prefix
                    and res_e.origin_asns != u_e.origin_asns
                ):
                    tmp.append(
                        mrt_entry(
                            filename=u_e.filename,
                            origin_asns=res_e.origin_asns.union(
                                u_e.origin_asns
                            ),
                            prefix=res_e.prefix,
                            timestamp=u_e.timestamp,
                        )
                    )

        if tmp:
            for tmp_e in tmp:
                if len(tmp_e.origin_asns) == len(
                    self.most_unreg_origins[0].origin_asns
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.most_unreg_origins
                    ]
                    if tmp_e.prefix not in s_prefixes:
                        self.most_unreg_origins.append(tmp_e)
                        changed = True
                elif len(tmp_e.origin_asns) > len(
                    self.most_unreg_origins[0].origin_asns
                ):
                    self.most_unreg_origins = [tmp_e]
                    changed = True
        else:
            if merge_data.most_unreg_origins:
                if self.most_unreg_origins:
                    if (
                        len(merge_data.most_unreg_origins[0].origin_asns)
                        == len(self.most_unreg_origins[0].origin_asns)
                        and len(self.most_unreg_origins[0].origin_asns) > 0
                    ):
                        s_prefixes = [
                            mrt_e.prefix for mrt_e in self.most_unreg_origins
                        ]
                        for mrt_e in merge_data.most_unreg_origins:
                            if mrt_e.prefix not in s_prefixes:
                                self.most_unreg_origins.append(mrt_e)
                                changed = True
                    elif len(
                        merge_data.most_unreg_origins[0].origin_asns
                    ) > len(self.most_unreg_origins[0].origin_asns):
                        self.most_unreg_origins = (
                            merge_data.most_unreg_origins.copy()
                        )
                        changed = True
                else:
                    self.most_unreg_origins = (
                        merge_data.most_unreg_origins.copy()
                    )
                    changed = True

        # Highest MED
        if merge_data.highest_med_prefixes:
            if self.highest_med_prefixes:
                if (
                    merge_data.highest_med_prefixes[0].med
                    == self.highest_med_prefixes[0].med
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.highest_med_prefixes
                    ]
                    s_med = self.highest_med_prefixes[0].med
                    for mrt_e in merge_data.highest_med_prefixes:
                        if mrt_e.prefix not in s_prefixes:
                            self.highest_med_prefixes.append(mrt_e)
                            changed = True
                elif (
                    merge_data.highest_med_prefixes[0].med
                    > self.highest_med_prefixes[0].med
                ):
                    self.highest_med_prefixes = (
                        merge_data.highest_med_prefixes.copy()
                    )
                    changed = True
            else:
                self.highest_med_prefixes = (
                    merge_data.highest_med_prefixes.copy()
                )
                changed = True

        # Invalid prefix length with most origin ASNs
        tmp = []
        for u_e in merge_data.invalid_len[:]:
            for res_e in self.invalid_len:
                if (
                    res_e.prefix == u_e.prefix
                    and res_e.origin_asns != u_e.origin_asns
                ):
                    tmp.append(
                        mrt_entry(
                            filename=u_e.filename,
                            origin_asns=res_e.origin_asns.union(
                                u_e.origin_asns
                            ),
                            prefix=res_e.prefix,
                            timestamp=u_e.timestamp,
                        )
                    )

        if tmp:
            for tmp_e in tmp:
                if len(tmp_e.origin_asns) == len(
                    self.invalid_len[0].origin_asns
                ):
                    s_prefixes = [mrt_e.prefix for mrt_e in self.invalid_len]
                    if tmp_e.prefix not in s_prefixes:
                        self.invalid_len.append(tmp_e)
                        changed = True
                elif len(tmp_e.origin_asns) > len(
                    self.invalid_len[0].origin_asns
                ):
                    self.invalid_len = [tmp_e]
                    changed = True
        else:
            if merge_data.invalid_len:
                if self.invalid_len:
                    if (
                        len(merge_data.invalid_len[0].origin_asns)
                        == len(self.invalid_len[0].origin_asns)
                        and len(self.invalid_len[0].origin_asns) > 0
                    ):
                        s_prefixes = [
                            mrt_e.prefix for mrt_e in self.invalid_len
                        ]
                        for mrt_e in merge_data.invalid_len:
                            if mrt_e.prefix not in s_prefixes:
                                self.invalid_len.append(mrt_e)
                                changed = True
                    elif len(merge_data.invalid_len[0].origin_asns) > len(
                        self.invalid_len[0].origin_asns
                    ):
                        self.invalid_len = merge_data.invalid_len.copy()
                        changed = True
                else:
                    self.invalid_len = merge_data.invalid_len.copy()
                    changed = True

        # Longest AS path
        if merge_data.longest_as_path:
            if self.longest_as_path:
                if len(merge_data.longest_as_path[0].as_path) == len(
                    self.longest_as_path[0].as_path
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.longest_as_path
                    ]
                    s_paths = [mrt_e.as_path for mrt_e in self.longest_as_path]
                    for mrt_e in merge_data.longest_as_path:
                        if mrt_e.prefix in s_prefixes:
                            if mrt_e.as_path not in s_paths:
                                self.longest_as_path.append(mrt_e)
                                changed = True
                        else:
                            self.longest_as_path.append(mrt_e)
                            changed = True
                elif len(merge_data.longest_as_path[0].as_path) > len(
                    self.longest_as_path[0].as_path
                ):
                    self.longest_as_path = merge_data.longest_as_path.copy()
                    changed = True
            else:
                self.longest_as_path = merge_data.longest_as_path.copy()
                changed = True

        # Longest community set
        if merge_data.longest_comm_set:
            if self.longest_comm_set:
                if len(merge_data.longest_comm_set[0].comm_set) == len(
                    self.longest_comm_set[0].comm_set
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.longest_comm_set
                    ]
                    s_comms = [
                        mrt_e.comm_set for mrt_e in self.longest_comm_set
                    ]
                    for mrt_e in merge_data.longest_comm_set:
                        if mrt_e.prefix in s_prefixes:
                            if mrt_e.comm_set not in s_comms:
                                self.longest_comm_set.append(mrt_e)
                                changed = True
                        else:
                            self.longest_comm_set.append(mrt_e)
                            changed = True
                elif len(merge_data.longest_comm_set[0].comm_set) > len(
                    self.longest_comm_set[0].comm_set
                ):
                    self.longest_comm_set = merge_data.longest_comm_set.copy()
                    changed = True
            else:
                self.longest_comm_set = merge_data.longest_comm_set.copy()
                changed = True

        # Most advertisements per prefix
        tmp = []
        # If stats from a rib dump are being added this wont be present:
        for u_e in merge_data.most_advt_prefixes[:]:
            for res_e in self.most_advt_prefixes:
                if res_e.prefix == u_e.prefix:
                    tmp.append(
                        mrt_entry(
                            advt=(res_e.advt + u_e.advt),
                            filename=u_e.filename,
                            prefix=res_e.prefix,
                            timestamp=u_e.timestamp,
                        )
                    )

        if tmp:
            for tmp_e in tmp:
                if tmp_e.advt == self.most_advt_prefixes[0].advt:
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.most_advt_prefixes
                    ]
                    if tmp_e.prefix not in s_prefixes:
                        self.most_advt_prefixes.append(tmp_e)
                        changed = True
                elif tmp_e.advt > self.most_advt_prefixes[0].advt:
                    self.most_advt_prefixes = [tmp_e]
                    changed = True
        else:
            if merge_data.most_advt_prefixes:
                if self.most_advt_prefixes:
                    if (
                        merge_data.most_advt_prefixes[0].advt
                        == self.most_advt_prefixes[0].advt
                        and self.most_advt_prefixes[0].advt > 0
                    ):
                        s_prefixes = [
                            mrt_e.prefix for mrt_e in self.most_advt_prefixes
                        ]
                        for mrt_e in merge_data.most_advt_prefixes:
                            if mrt_e.prefix not in s_prefixes:
                                self.most_advt_prefixes.append(mrt_e)
                                changed = True
                    elif (
                        merge_data.most_advt_prefixes[0].advt
                        > self.most_advt_prefixes[0].advt
                    ):
                        self.most_advt_prefixes = (
                            merge_data.most_advt_prefixes.copy()
                        )
                        changed = True
                else:
                    self.most_advt_prefixes = (
                        merge_data.most_advt_prefixes.copy()
                    )
                    changed = True

        # Most bogon ASNs per origin ASN
        tmp = []
        # If stats from a rib dump are being added this wont be present:
        for u_e in merge_data.most_bogon_asns[:]:
            for res_e in self.most_bogon_asns:
                if res_e.as_path == u_e.as_path:
                    tmp.append(
                        mrt_entry(
                            as_path=res_e.as_path,
                            origin_asns=res_e.origin_asns.union(
                                u_e.origin_asns
                            ),
                            filename=res_e.filename,
                            timestamp=res_e.timestamp,
                        )
                    )

        if tmp:
            for tmp_e in tmp:
                if len(tmp_e.origin_asns) == len(
                    self.most_bogon_asns[0].origin_asns
                ):
                    s_asns = [mrt_e.as_path for mrt_e in self.most_bogon_asns]
                    if tmp_e.as_path not in s_asns:
                        self.most_bogon_asns.append(tmp_e)
                        changed = True
                elif len(tmp_e.origin_asns) > len(
                    self.most_bogon_asns[0].origin_asns
                ):
                    self.most_bogon_asns = [tmp_e]
                    changed = True
        else:
            if merge_data.most_bogon_asns:
                if self.most_bogon_asns:
                    if (
                        len(merge_data.most_bogon_asns[0].origin_asns)
                        == len(self.most_bogon_asns[0].origin_asns)
                        and len(self.most_bogon_asns[0].origin_asns) > 0
                    ):
                        s_asns = [
                            mrt_e.as_path for mrt_e in self.most_bogon_asns
                        ]
                        for mrt_e in merge_data.most_bogon_asns:
                            if mrt_e.as_path not in s_asns:
                                self.most_bogon_asns.append(mrt_e)
                                changed = True
                    elif len(merge_data.most_bogon_asns[0].origin_asns) > len(
                        self.most_bogon_asns[0].origin_asns
                    ):
                        self.most_bogon_asns = (
                            merge_data.most_bogon_asns.copy()
                        )
                        changed = True
                else:
                    self.most_bogon_asns = merge_data.most_bogon_asns.copy()
                    changed = True

        # Most updates per prefix
        tmp = []
        # If stats from a rib dump are being added this wont be present:
        for u_e in merge_data.most_upd_prefixes[:]:
            for res_e in self.most_upd_prefixes:
                if res_e.prefix == u_e.prefix:
                    tmp.append(
                        mrt_entry(
                            filename=u_e.filename,
                            prefix=res_e.prefix,
                            timestamp=u_e.timestamp,
                            updates=(res_e.updates + u_e.updates),
                        )
                    )

        if tmp:
            for tmp_e in tmp:
                if tmp_e.updates == self.most_upd_prefixes[0].updates:
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.most_upd_prefixes
                    ]
                    if tmp_e.prefix not in s_prefixes:
                        self.most_upd_prefixes.append(tmp_e)
                        changed = True
                elif tmp_e.updates > self.most_upd_prefixes[0].updates:
                    self.most_upd_prefixes = [tmp_e]
                    changed = True
        else:
            if merge_data.most_upd_prefixes:
                if self.most_upd_prefixes:
                    if (
                        merge_data.most_upd_prefixes[0].updates
                        == self.most_upd_prefixes[0].updates
                        and self.most_upd_prefixes[0].updates > 0
                    ):
                        s_prefixes = [
                            mrt_e.prefix for mrt_e in self.most_upd_prefixes
                        ]
                        for mrt_e in merge_data.most_upd_prefixes:
                            if mrt_e.prefix not in s_prefixes:
                                self.most_upd_prefixes.append(mrt_e)
                                changed = True
                    elif (
                        merge_data.most_upd_prefixes[0].updates
                        > self.most_upd_prefixes[0].updates
                    ):
                        self.most_upd_prefixes = (
                            merge_data.most_upd_prefixes.copy()
                        )
                        changed = True
                else:
                    self.most_upd_prefixes = (
                        merge_data.most_upd_prefixes.copy()
                    )
                    changed = True

        # Most withdraws per prefix
        tmp = []
        # If stats from a rib dump are being added this wont be present:
        for u_e in merge_data.most_withd_prefixes[:]:
            for res_e in self.most_withd_prefixes:
                if res_e.prefix == u_e.prefix:
                    tmp.append(
                        mrt_entry(
                            filename=u_e.filename,
                            prefix=res_e.prefix,
                            timestamp=u_e.timestamp,
                            withdraws=(res_e.withdraws + u_e.withdraws),
                        )
                    )

        if tmp:
            for tmp_e in tmp:
                if tmp_e.withdraws == self.most_withd_prefixes[0].withdraws:
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.most_withd_prefixes
                    ]
                    if tmp_e.prefix not in s_prefixes:
                        self.most_withd_prefixes.append(tmp_e)
                        changed = True
                elif tmp_e.withdraws > self.most_withd_prefixes[0].withdraws:
                    self.most_withd_prefixes = [tmp_e]
                    changed = True
        else:
            if merge_data.most_withd_prefixes:
                if self.most_withd_prefixes:
                    if (
                        merge_data.most_withd_prefixes[0].withdraws
                        == self.most_withd_prefixes[0].withdraws
                        and self.most_withd_prefixes[0].withdraws > 0
                    ):
                        s_prefixes = [
                            mrt_e.prefix for mrt_e in self.most_withd_prefixes
                        ]
                        for mrt_e in merge_data.most_withd_prefixes:
                            if mrt_e.prefix not in s_prefixes:
                                self.most_withd_prefixes.append(mrt_e)
                                changed = True
                    elif (
                        merge_data.most_withd_prefixes[0].withdraws
                        > self.most_withd_prefixes[0].withdraws
                    ):
                        self.most_withd_prefixes = (
                            merge_data.most_withd_prefixes.copy()
                        )
                        changed = True
                else:
                    self.most_withd_prefixes = (
                        merge_data.most_withd_prefixes.copy()
                    )
                    changed = True

        # Most advertisement per origin ASN
        tmp = []
        # If stats from a rib dump are being added this wont be present:
        for u_e in merge_data.most_advt_origin_asn[:]:
            for res_e in self.most_advt_origin_asn:
                if res_e.origin_asns == u_e.origin_asns:
                    tmp.append(
                        mrt_entry(
                            advt=(res_e.advt + u_e.advt),
                            filename=u_e.filename,
                            origin_asns=res_e.origin_asns,
                            timestamp=u_e.timestamp,
                        )
                    )

        if tmp:
            for tmp_e in tmp:
                if tmp_e.advt == self.most_advt_origin_asn[0].advt:
                    s_origin_asns = [
                        mrt_e.origin_asns
                        for mrt_e in self.most_advt_origin_asn
                    ]
                    if tmp_e.origin_asns not in s_origin_asns:
                        self.most_advt_origin_asn.append(tmp_e)
                        changed = True
                elif tmp_e.advt > self.most_advt_origin_asn[0].advt:
                    self.most_advt_origin_asn = [tmp_e]
                    changed = True
        else:
            if merge_data.most_advt_origin_asn:
                if self.most_advt_origin_asn:
                    if (
                        merge_data.most_advt_origin_asn[0].advt
                        == self.most_advt_origin_asn[0].advt
                        and self.most_advt_origin_asn[0].advt > 0
                    ):
                        s_origin_asns = [
                            mrt_e.origin_asns
                            for mrt_e in self.most_advt_origin_asn
                        ]
                        for mrt_e in merge_data.most_advt_origin_asn:
                            if mrt_e.origin_asns not in s_origin_asns:
                                self.most_advt_origin_asn.append(mrt_e)
                                changed = True
                    elif (
                        merge_data.most_advt_origin_asn[0].advt
                        > self.most_advt_origin_asn[0].advt
                    ):
                        self.most_advt_origin_asn = (
                            merge_data.most_advt_origin_asn.copy()
                        )
                        changed = True
                else:
                    self.most_advt_origin_asn = (
                        merge_data.most_advt_origin_asn.copy()
                    )
                    changed = True

        # Most advertisement per peer ASN
        tmp = []
        # If stats from a rib dump are being added this wont be present:
        for u_e in merge_data.most_advt_peer_asn[:]:
            for res_e in self.most_advt_peer_asn:
                if res_e.peer_asn == u_e.peer_asn:
                    tmp.append(
                        mrt_entry(
                            advt=(res_e.advt + u_e.advt),
                            filename=u_e.filename,
                            peer_asn=res_e.peer_asn,
                            timestamp=u_e.timestamp,
                        )
                    )

        if tmp:
            for tmp_e in tmp:
                if tmp_e.advt == self.most_advt_peer_asn[0].advt:
                    s_peer_asns = [
                        mrt_e.peer_asn for mrt_e in self.most_advt_peer_asn
                    ]
                    if tmp_e.peer_asn not in s_peer_asns:
                        self.most_advt_peer_asn.append(tmp_e)
                        changed = True
                elif tmp_e.advt > self.most_advt_peer_asn[0].advt:
                    self.most_advt_peer_asn = [tmp_e]
                    changed = True
        else:
            if merge_data.most_advt_peer_asn:
                if self.most_advt_peer_asn:
                    if (
                        merge_data.most_advt_peer_asn[0].advt
                        == self.most_advt_peer_asn[0].advt
                        and self.most_advt_peer_asn[0].advt > 0
                    ):
                        s_peer_asns = [
                            mrt_e.peer_asn for mrt_e in self.most_advt_peer_asn
                        ]
                        for mrt_e in merge_data.most_advt_peer_asn:
                            if mrt_e.peer_asn not in s_peer_asns:
                                self.most_advt_peer_asn.append(mrt_e)
                                changed = True
                    elif (
                        merge_data.most_advt_peer_asn[0].advt
                        > self.most_advt_peer_asn[0].advt
                    ):
                        self.most_advt_peer_asn = (
                            merge_data.most_advt_peer_asn.copy()
                        )
                        changed = True
                else:
                    self.most_advt_peer_asn = (
                        merge_data.most_advt_peer_asn.copy()
                    )
                    changed = True

        # Most updates per peer ASN
        tmp = []
        # If stats from a rib dump are being added this wont be present:
        for u_e in merge_data.most_upd_peer_asn[:]:
            for res_e in self.most_upd_peer_asn:
                if res_e.peer_asn == u_e.peer_asn:
                    tmp.append(
                        mrt_entry(
                            filename=u_e.filename,
                            peer_asn=res_e.peer_asn,
                            timestamp=u_e.timestamp,
                            updates=(res_e.updates + u_e.updates),
                        )
                    )

        if tmp:
            for tmp_e in tmp:
                if tmp_e.updates == self.most_upd_peer_asn[0].updates:
                    s_peer_asns = [
                        mrt_e.peer_asn for mrt_e in self.most_upd_peer_asn
                    ]
                    if tmp_e.peer_asn not in s_peer_asns:
                        self.most_upd_peer_asn.append(tmp_e)
                        changed = True
                elif tmp_e.updates > self.most_upd_peer_asn[0].updates:
                    self.most_upd_peer_asn = [tmp_e]
                    changed = True
        else:
            if merge_data.most_upd_peer_asn:
                if self.most_upd_peer_asn:
                    if (
                        merge_data.most_upd_peer_asn[0].updates
                        == self.most_upd_peer_asn[0].updates
                        and self.most_upd_peer_asn[0].updates > 0
                    ):
                        s_peer_asns = [
                            mrt_e.peer_asn for mrt_e in self.most_upd_peer_asn
                        ]
                        for mrt_e in merge_data.most_upd_peer_asn:
                            if mrt_e.peer_asn not in s_peer_asns:
                                self.most_upd_peer_asn.append(mrt_e)
                                changed = True
                    elif (
                        merge_data.most_upd_peer_asn[0].updates
                        > self.most_upd_peer_asn[0].updates
                    ):
                        self.most_upd_peer_asn = (
                            merge_data.most_upd_peer_asn.copy()
                        )
                        changed = True
                else:
                    self.most_upd_peer_asn = (
                        merge_data.most_upd_peer_asn.copy()
                    )
                    changed = True

        # Most withdraws per peer ASN
        tmp = []
        # If stats from a rib dump are being added this wont be present:
        for u_e in merge_data.most_withd_peer_asn[:]:
            for res_e in self.most_withd_peer_asn:
                if res_e.peer_asn == u_e.peer_asn:
                    tmp.append(
                        mrt_entry(
                            filename=u_e.filename,
                            peer_asn=res_e.peer_asn,
                            timestamp=u_e.timestamp,
                            withdraws=(res_e.withdraws + u_e.withdraws),
                        )
                    )

        if tmp:
            for tmp_e in tmp:
                if tmp_e.withdraws == self.most_withd_peer_asn[0].withdraws:
                    s_peer_asns = [
                        mrt_e.peer_asn for mrt_e in self.most_withd_peer_asn
                    ]
                    if tmp_e.peer_asn not in s_peer_asns:
                        self.most_withd_peer_asn.append(tmp_e)
                        changed = True
                elif tmp_e.withdraws > self.most_withd_peer_asn[0].withdraws:
                    self.most_withd_peer_asn = [tmp_e]
                    changed = True
        else:
            if merge_data.most_withd_peer_asn:
                if self.most_withd_peer_asn:
                    if (
                        merge_data.most_withd_peer_asn[0].withdraws
                        == self.most_withd_peer_asn[0].withdraws
                        and self.most_withd_peer_asn[0].withdraws > 0
                    ):
                        s_peer_asns = [
                            mrt_e.peer_asn
                            for mrt_e in self.most_withd_peer_asn
                        ]
                        for mrt_e in merge_data.most_withd_peer_asn:
                            if mrt_e.peer_asn not in s_peer_asns:
                                self.most_withd_peer_asn.append(mrt_e)
                                changed = True
                    elif (
                        merge_data.most_withd_peer_asn[0].withdraws
                        > self.most_withd_peer_asn[0].withdraws
                    ):
                        self.most_withd_peer_asn = (
                            merge_data.most_withd_peer_asn.copy()
                        )
                        changed = True
                else:
                    self.most_withd_peer_asn = (
                        merge_data.most_withd_peer_asn.copy()
                    )
                    changed = True

        # Most origin ASNs per prefix
        tmp = []

        self_prefixes: dict[str, None] = {}
        for mrt_e in self.most_origin_asns:
            self_prefixes[mrt_e.prefix] = None
        # ^ This is a hack to speed up this section up:
        for mrt_e in merge_data.most_origin_asns:
            if mrt_e.prefix not in self_prefixes:
                continue
            for s_e in self.most_origin_asns:
                if (
                    s_e.prefix == mrt_e.prefix
                    and s_e.origin_asns != mrt_e.origin_asns
                ):
                    tmp.append(
                        mrt_entry(
                            filename=mrt_e.filename,
                            origin_asns=s_e.origin_asns.union(
                                mrt_e.origin_asns
                            ),
                            prefix=s_e.prefix,
                            timestamp=mrt_e.timestamp,
                        )
                    )
                    break

        if tmp:
            for tmp_e in tmp:
                if len(tmp_e.origin_asns) == len(
                    self.most_origin_asns[0].origin_asns
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.most_origin_asns
                    ]
                    if tmp_e.prefix not in s_prefixes:
                        self.most_origin_asns.append(tmp_e)
                        changed = True
                elif len(tmp_e.origin_asns) > len(
                    self.most_origin_asns[0].origin_asns
                ):
                    self.most_origin_asns = [tmp_e]
                    changed = True
        else:
            if merge_data.most_origin_asns:
                if self.most_origin_asns:
                    if (
                        len(merge_data.most_origin_asns[0].origin_asns)
                        == len(self.most_origin_asns[0].origin_asns)
                        and len(self.most_origin_asns[0].origin_asns) > 0
                    ):
                        s_prefixes = [
                            mrt_e.prefix for mrt_e in self.most_origin_asns
                        ]
                        for mrt_e in merge_data.most_origin_asns:
                            if mrt_e.prefix not in s_prefixes:
                                self.most_origin_asns.append(mrt_e)
                                changed = True
                    elif len(merge_data.most_origin_asns[0].origin_asns) > len(
                        self.most_origin_asns[0].origin_asns
                    ):
                        self.most_origin_asns = (
                            merge_data.most_origin_asns.copy()
                        )
                        changed = True
                else:
                    self.most_origin_asns = merge_data.most_origin_asns.copy()
                    changed = True

        # Most unknown attributes per prefix
        tmp = []
        # If stats from a rib dump are being added this wont be present:
        for u_e in merge_data.most_unknown_attrs:
            for res_e in self.most_unknown_attrs:
                if res_e.prefix == u_e.prefix:
                    tmp.append(
                        mrt_entry(
                            origin_asns=res_e.origin_asns.union(
                                u_e.origin_asns
                            ),
                            prefix=res_e.prefix,
                            unknown_attrs=res_e.unknown_attrs.union(
                                u_e.unknown_attrs
                            ),
                            filename=res_e.filename,
                            timestamp=res_e.timestamp,
                        )
                    )

        for tmp_e in tmp:
            if len(tmp_e.unknown_attrs) == len(
                self.most_unknown_attrs[0].unknown_attrs
            ):
                s_prefixes = [
                    mrt_e.prefix for mrt_e in self.most_unknown_attrs
                ]
                if tmp_e.prefix not in s_prefixes:
                    self.most_unknown_attrs.append(tmp_e)
                    changed = True
            elif len(tmp_e.unknown_attrs) > len(
                self.most_unknown_attrs[0].unknown_attrs
            ):
                self.most_unknown_attrs = [tmp_e]
                changed = True
        else:
            if merge_data.most_unknown_attrs:
                if self.most_unknown_attrs:
                    if (
                        len(merge_data.most_unknown_attrs[0].unknown_attrs)
                        == len(self.most_unknown_attrs[0].unknown_attrs)
                        and len(self.most_unknown_attrs[0].unknown_attrs) > 0
                    ):
                        s_prefixes = [
                            mrt_e.prefix for mrt_e in self.most_unknown_attrs
                        ]
                        for mrt_e in merge_data.most_unknown_attrs:
                            if mrt_e.prefix not in s_prefixes:
                                self.most_unknown_attrs.append(mrt_e)
                                changed = True
                    elif len(
                        merge_data.most_unknown_attrs[0].unknown_attrs
                    ) > len(self.most_unknown_attrs[0].unknown_attrs):
                        self.most_unknown_attrs = (
                            merge_data.most_unknown_attrs.copy()
                        )
                        changed = True
                else:
                    self.most_unknown_attrs = (
                        merge_data.most_unknown_attrs.copy()
                    )
                    changed = True

        # If stats from a rib dump are being added, these will be 0:
        if merge_data.total_upd:
            self.total_upd += merge_data.total_upd
            changed = True

        if merge_data.total_advt:
            self.total_advt += merge_data.total_advt
            changed = True

        if merge_data.total_withd:
            self.total_withd += merge_data.total_withd
            changed = True

        if changed:
            for filename in merge_data.file_list:
                if filename not in self.file_list:
                    self.file_list.append(filename)
            self.timestamp = merge_data.timestamp

        return changed

    def add_archive(self: "mrt_stats", name: str) -> None:
        """
        Add the name of an MRT archive to the list if it isn't already present.
        """
        if not name:
            raise ValueError(f"name is required for")
        self.archive_list.add(name)

    def equal_to(
        self: "mrt_stats", mrt_s: "mrt_stats", meta: bool = False
    ) -> bool:
        """
        Return True if this MRT stats obj is the same as mrt_s, else False.
        Comparing meta data like file list or timestamp is optional.
        """
        if not mrt_s:
            raise ValueError(f"Missing required options: mrt_s={mrt_s}")

        if type(mrt_s) != mrt_stats:
            raise TypeError(f"mrt_s is not a stats object: {type(mrt_s)}")

        if len(self.bogon_origin_asns) != len(mrt_s.bogon_origin_asns):
            return False

        for self_e in self.bogon_origin_asns:
            for mrt_e in mrt_s.bogon_origin_asns[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.bogon_origin_asns.remove(mrt_e)
                    break
        if mrt_s.bogon_origin_asns:
            return False

        if len(self.bogon_prefixes) != len(mrt_s.bogon_prefixes):
            return False

        for self_e in self.bogon_prefixes:
            for mrt_e in mrt_s.bogon_prefixes[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.bogon_prefixes.remove(mrt_e)
                    break
        if mrt_s.bogon_prefixes:
            return False

        if len(self.highest_med_prefixes) != len(mrt_s.highest_med_prefixes):
            return False

        for self_e in self.highest_med_prefixes:
            for mrt_e in mrt_s.highest_med_prefixes[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.highest_med_prefixes.remove(mrt_e)
                    break
        if mrt_s.highest_med_prefixes:
            return False

        if len(self.invalid_len) != len(mrt_s.invalid_len):
            return False

        if len(self.longest_as_path) != len(mrt_s.longest_as_path):
            return False

        for self_e in self.longest_as_path:
            for mrt_e in mrt_s.longest_as_path[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.longest_as_path.remove(mrt_e)
                    break
        if mrt_s.longest_as_path:
            return False

        if len(self.longest_comm_set) != len(mrt_s.longest_comm_set):
            return False

        for self_e in self.longest_comm_set:
            for mrt_e in mrt_s.longest_comm_set[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.longest_comm_set.remove(mrt_e)
                    break
        if mrt_s.longest_comm_set:
            return False

        for self_e in self.invalid_len:
            for mrt_e in mrt_s.invalid_len[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.invalid_len.remove(mrt_e)
                    break
        if mrt_s.invalid_len:
            return False

        if len(self.most_bogon_asns) != len(mrt_s.most_bogon_asns):
            return False

        for self_e in self.most_bogon_asns:
            for mrt_e in mrt_s.most_bogon_asns[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_bogon_asns.remove(mrt_e)
                    break
        if mrt_s.most_bogon_asns:
            return False

        if len(self.most_advt_prefixes) != len(mrt_s.most_advt_prefixes):
            return False

        for self_e in self.most_advt_prefixes:
            for mrt_e in mrt_s.most_advt_prefixes[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_advt_prefixes.remove(mrt_e)
                    break
        if mrt_s.most_advt_prefixes:
            return False

        if len(self.most_upd_prefixes) != len(mrt_s.most_upd_prefixes):
            return False

        for self_e in self.most_upd_prefixes:
            for mrt_e in mrt_s.most_upd_prefixes[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_upd_prefixes.remove(mrt_e)
                    break
        if mrt_s.most_upd_prefixes:
            return False

        if len(self.most_withd_prefixes) != len(mrt_s.most_withd_prefixes):
            return False

        for self_e in self.most_withd_prefixes:
            for mrt_e in mrt_s.most_withd_prefixes[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_withd_prefixes.remove(mrt_e)
                    break
        if mrt_s.most_withd_prefixes:
            return False

        if len(self.most_advt_origin_asn) != len(mrt_s.most_advt_origin_asn):
            return False

        for self_e in self.most_advt_origin_asn:
            for mrt_e in mrt_s.most_advt_origin_asn[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_advt_origin_asn.remove(mrt_e)
                    break
        if mrt_s.most_advt_origin_asn:
            return False

        if len(self.most_advt_peer_asn) != len(mrt_s.most_advt_peer_asn):
            return False

        for self_e in self.most_advt_peer_asn:
            for mrt_e in mrt_s.most_advt_peer_asn[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_advt_peer_asn.remove(mrt_e)
                    break
        if mrt_s.most_advt_peer_asn:
            return False

        if len(self.most_upd_peer_asn) != len(mrt_s.most_upd_peer_asn):
            return False

        for self_e in self.most_upd_peer_asn:
            for mrt_e in mrt_s.most_upd_peer_asn[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_upd_peer_asn.remove(mrt_e)
                    break
        if mrt_s.most_upd_peer_asn:
            return False

        if len(self.most_withd_peer_asn) != len(mrt_s.most_withd_peer_asn):
            return False

        for self_e in self.most_withd_peer_asn:
            for mrt_e in mrt_s.most_withd_peer_asn[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_withd_peer_asn.remove(mrt_e)
                    break
        if mrt_s.most_withd_peer_asn:
            return False

        if len(self.most_origin_asns) != len(mrt_s.most_origin_asns):
            return False

        for self_e in self.most_origin_asns:
            for mrt_e in mrt_s.most_origin_asns[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_origin_asns.remove(mrt_e)
                    break
        if mrt_s.most_origin_asns:
            return False

        if len(self.most_unknown_attrs) != len(mrt_s.most_unknown_attrs):
            return False

        for self_e in self.most_unknown_attrs:
            for mrt_e in mrt_s.most_unknown_attrs[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_unknown_attrs.remove(mrt_e)
                    break
        if mrt_s.most_unknown_attrs:
            return False

        for self_e in self.most_unreg_origins:
            for mrt_e in mrt_s.most_unreg_origins[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_unreg_origins.remove(mrt_e)
                    break
        if mrt_s.most_unreg_origins:
            return False

        if self.total_upd != mrt_s.total_upd:
            return False

        if self.total_advt != mrt_s.total_advt:
            return False

        if self.total_withd != mrt_s.total_withd:
            return False

        if meta:
            if self.file_list != mrt_s.file_list:
                return False

            if self.timestamp != mrt_s.timestamp:
                return False

        return True

    def from_file(self: "mrt_stats", filename: str) -> None:
        """
        Load and parse MRT stats obj from a JSON text file.
        """
        if not filename:
            raise ValueError(f"Missing required options: filename={filename}")

        if type(filename) != str:
            raise TypeError(f"filename is not a string: {type(filename)}")

        with open(filename, "r") as f:
            self.from_json(f.read())

    def from_json(self: "mrt_stats", json_str: str) -> None:
        """
        Parse the JSON string as MRT stats data.
        To provide backward connectivity with old data in Redis, need to check
        if some newer keys are present in the JSON dict stored in Redis.
        """
        if not json_str:
            raise ValueError(f"Missing required options: json_str={json_str}")

        if type(json_str) != str:
            raise TypeError(f"json_str is not a string: {type(json_str)}")

        json_dict = json.loads(json_str)

        if "archive_list" in json_dict:
            self.archive_list = set(json_dict["archive_list"])
        else:
            self.archive_list = set()

        self.bogon_origin_asns = []
        if "bogon_origin_asns" in json_dict:
            for json_e in json_dict["bogon_origin_asns"]:
                mrt_e = mrt_entry()
                mrt_e.from_json(json_e)
                self.bogon_origin_asns.append(mrt_e)
        # else:
        #    self.bogon_origin_asns.append(mrt_entry())
        # Breaks report generations -> remove if stable after commenting out

        self.bogon_prefixes = []
        if "bogon_prefixes" in json_dict:
            for json_e in json_dict["bogon_prefixes"]:
                mrt_e = mrt_entry()
                mrt_e.from_json(json_e)
                self.bogon_prefixes.append(mrt_e)
        # else:
        #    self.bogon_prefixes.append(mrt_entry())
        # Breaks report generations -> remove if stable after commenting out

        self.highest_med_prefixes = []
        if "highest_med_prefixes" in json_dict:
            for json_e in json_dict["highest_med_prefixes"]:
                mrt_e = mrt_entry()
                mrt_e.from_json(json_e)
                self.highest_med_prefixes.append(mrt_e)
        # else:
        #    self.highest_med_prefixes.append(mrt_entry())
        # Breaks report generations -> remove if stable after commenting out

        self.invalid_len = []
        if "invalid_len" in json_dict:
            for json_e in json_dict["invalid_len"]:
                mrt_e = mrt_entry()
                mrt_e.from_json(json_e)
                self.invalid_len.append(mrt_e)
        # else:
        #    self.invalid_len.append(mrt_entry())
        # Breaks report generations -> remove if stable after commenting out

        self.longest_as_path = []
        for json_e in json_dict["longest_as_path"]:
            mrt_e = mrt_entry()
            mrt_e.from_json(json_e)
            self.longest_as_path.append(mrt_e)

        self.longest_comm_set = []
        for json_e in json_dict["longest_comm_set"]:
            mrt_e = mrt_entry()
            mrt_e.from_json(json_e)
            self.longest_comm_set.append(mrt_e)

        self.most_advt_prefixes = []
        for json_e in json_dict["most_advt_prefixes"]:
            mrt_e = mrt_entry()
            mrt_e.from_json(json_e)
            self.most_advt_prefixes.append(mrt_e)

        self.most_bogon_asns = []
        if "most_bogon_asns" in json_dict:
            for json_e in json_dict["most_bogon_asns"]:
                mrt_e = mrt_entry()
                mrt_e.from_json(json_e)
                self.most_bogon_asns.append(mrt_e)
        # else:
        #    self.most_bogon_asns.append(mrt_entry())
        # Breaks report generations -> remove if stable after commenting out

        self.most_upd_prefixes = []
        for json_e in json_dict["most_upd_prefixes"]:
            mrt_e = mrt_entry()
            mrt_e.from_json(json_e)
            self.most_upd_prefixes.append(mrt_e)

        self.most_withd_prefixes = []
        for json_e in json_dict["most_withd_prefixes"]:
            mrt_e = mrt_entry()
            mrt_e.from_json(json_e)
            self.most_withd_prefixes.append(mrt_e)

        self.most_advt_origin_asn = []
        for json_e in json_dict["most_advt_origin_asn"]:
            mrt_e = mrt_entry()
            mrt_e.from_json(json_e)
            self.most_advt_origin_asn.append(mrt_e)

        self.most_advt_peer_asn = []
        for json_e in json_dict["most_advt_peer_asn"]:
            mrt_e = mrt_entry()
            mrt_e.from_json(json_e)
            self.most_advt_peer_asn.append(mrt_e)

        self.most_upd_peer_asn = []
        for json_e in json_dict["most_upd_peer_asn"]:
            mrt_e = mrt_entry()
            mrt_e.from_json(json_e)
            self.most_upd_peer_asn.append(mrt_e)

        self.most_withd_peer_asn = []
        for json_e in json_dict["most_withd_peer_asn"]:
            mrt_e = mrt_entry()
            mrt_e.from_json(json_e)
            self.most_withd_peer_asn.append(mrt_e)

        self.most_origin_asns = []
        for json_e in json_dict["most_origin_asns"]:
            mrt_e = mrt_entry()
            mrt_e.from_json(json_e)
            self.most_origin_asns.append(mrt_e)

        self.most_unknown_attrs = []
        if "most_unknown_attrs" in json_dict:
            for json_e in json_dict["most_unknown_attrs"]:
                mrt_e = mrt_entry()
                mrt_e.from_json(json_e)
                self.most_unknown_attrs.append(mrt_e)
        # else:
        #    self.most_unknown_attrs.append(mrt_entry())
        # Breaks report generations -> remove if stable after commenting out

        self.most_unreg_origins = []
        if "most_unreg_origins" in json_dict:
            for json_e in json_dict["most_unreg_origins"]:
                mrt_e = mrt_entry()
                mrt_e.from_json(json_e)
                self.most_unreg_origins.append(mrt_e)
        # else:
        #    self.most_unreg_origins.append(mrt_entry())
        # Breaks report generations -> remove if stable after commenting out

        self.file_list = json_dict["file_list"]

        self.timestamp = json_dict["timestamp"]

        self.total_upd = 0
        if "total_upd" in json_dict:
            self.total_upd = int(json_dict["total_upd"])

        self.total_advt = 0
        if "total_advt" in json_dict:
            self.total_advt = int(json_dict["total_advt"])

        self.total_withd = 0
        if "total_withd" in json_dict:
            self.total_withd = int(json_dict["total_withd"])

    @staticmethod
    def gen_ts_from_ymd(ymd: str) -> str:
        """
        Generate and return the timestamp for a specific day, for use when
        creating an mrt_stats objects which contains the summary data for a
        whole day.
        """
        if not ymd:
            raise ValueError(f"Missing required arguments: ymd={ymd}")

        if type(ymd) != str:
            raise TypeError(f"ymd is not a string: {type(ymd)}")

        mrt_archive.valid_ymd(ymd)

        return datetime.datetime.strftime(
            datetime.datetime.strptime(ymd, cfg.DAY_FORMAT), cfg.TIME_FORMAT
        )

    @staticmethod
    def gen_daily_key(ymd: str) -> str:
        """
        Generate the redis key used to store the global stats obj for a
        specific day.
        """
        if not ymd:
            raise ValueError(f"Missing required arguments: ymd={ymd}")

        if type(ymd) != str:
            raise TypeError(f"ymd is not a string: {type(ymd)}")

        mrt_archive.valid_ymd(ymd)

        return "DAILY:" + ymd

    @staticmethod
    def gen_diff_key(ymd: str) -> str:
        """
        Generate the redis key used to store the diff stats obj for a
        specific day.
        """
        if not ymd:
            raise ValueError(f"Missing required arguments: ymd={ymd}")

        if type(ymd) != str:
            raise TypeError(f"ymd is not a string: {type(ymd)}")

        mrt_archive.valid_ymd(ymd)

        return "DAILY_DIFF:" + ymd

    @staticmethod
    def gen_global_key() -> str:
        """
        Generate the key used to store the running global stats obj in redis.
        """
        return "GLOBAL"

    def get_diff(self: "mrt_stats", mrt_s: "mrt_stats") -> "mrt_stats":
        """
        Generate an mrt_stats obj with entries unique to mrt_s.
        Don't diff meta data like timestamp or file list.
        """
        if not mrt_s:
            raise ValueError(f"Missing required options: mrt_s={mrt_s}")

        if type(mrt_s) != mrt_stats:
            raise TypeError(f"mrt_s is not a stats object: {type(mrt_s)}")

        diff = mrt_stats()
        diff.bogon_origin_asns = []
        diff.bogon_prefixes = []
        diff.highest_med_prefixes = []
        diff.invalid_len = []
        diff.longest_as_path = []
        diff.longest_comm_set = []
        diff.most_advt_prefixes = []
        diff.most_bogon_asns = []
        diff.most_upd_prefixes = []
        diff.most_withd_prefixes = []
        diff.most_advt_origin_asn = []
        diff.most_advt_peer_asn = []
        diff.most_upd_peer_asn = []
        diff.most_withd_peer_asn = []
        diff.most_origin_asns = []
        diff.most_unknown_attrs = []
        diff.most_unreg_origins = []

        for mrt_e in mrt_s.bogon_origin_asns:
            found = False
            for self_e in self.bogon_origin_asns:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.bogon_origin_asns.append(mrt_e)

        for mrt_e in mrt_s.bogon_prefixes:
            found = False
            for self_e in self.bogon_prefixes:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.bogon_prefixes.append(mrt_e)

        for mrt_e in mrt_s.highest_med_prefixes:
            found = False
            for self_e in self.highest_med_prefixes:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.highest_med_prefixes.append(mrt_e)

        for mrt_e in mrt_s.invalid_len:
            found = False
            for self_e in self.invalid_len:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.invalid_len.append(mrt_e)

        for mrt_e in mrt_s.invalid_len:
            found = False
            for self_e in self.invalid_len:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.invalid_len.append(mrt_e)

        for mrt_e in mrt_s.longest_as_path:
            found = False
            for self_e in self.longest_as_path:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.longest_as_path.append(mrt_e)

        for mrt_e in mrt_s.longest_comm_set:
            found = False
            for self_e in self.longest_comm_set:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.longest_comm_set.append(mrt_e)

        for mrt_e in mrt_s.most_advt_prefixes:
            found = False
            for self_e in self.most_advt_prefixes:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.most_advt_prefixes.append(mrt_e)

        for mrt_e in mrt_s.most_bogon_asns:
            found = False
            for self_e in self.most_bogon_asns:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.most_bogon_asns.append(mrt_e)

        for mrt_e in mrt_s.most_upd_prefixes:
            found = False
            for self_e in self.most_upd_prefixes:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.most_upd_prefixes.append(mrt_e)

        for mrt_e in mrt_s.most_withd_prefixes:
            found = False
            for self_e in self.most_withd_prefixes:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.most_withd_prefixes.append(mrt_e)

        for mrt_e in mrt_s.most_advt_origin_asn:
            found = False
            for self_e in self.most_advt_origin_asn:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.most_advt_origin_asn.append(mrt_e)

        for mrt_e in mrt_s.most_advt_peer_asn:
            found = False
            for self_e in self.most_advt_peer_asn:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.most_advt_peer_asn.append(mrt_e)

        for mrt_e in mrt_s.most_upd_peer_asn:
            found = False
            for self_e in self.most_upd_peer_asn:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.most_upd_peer_asn.append(mrt_e)

        for mrt_e in mrt_s.most_withd_peer_asn:
            found = False
            for self_e in self.most_withd_peer_asn:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.most_withd_peer_asn.append(mrt_e)

        for mrt_e in mrt_s.most_origin_asns:
            found = False
            for self_e in self.most_origin_asns:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.most_origin_asns.append(mrt_e)

        for mrt_e in mrt_s.most_unknown_attrs:
            found = False
            for self_e in self.most_unknown_attrs:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.most_unknown_attrs.append(mrt_e)

        for mrt_e in mrt_s.most_unreg_origins:
            found = False
            for self_e in self.most_unreg_origins:
                if self_e.equal_to(mrt_e):
                    found = True
                    break
            if not found:
                diff.most_unreg_origins.append(mrt_e)

        if mrt_s.total_upd != self.total_upd:
            diff.total_upd = mrt_s.total_upd

        if mrt_s.total_advt != self.total_advt:
            diff.total_advt = mrt_s.total_advt

        if mrt_s.total_withd != self.total_withd:
            diff.total_withd = mrt_s.total_withd

        return diff

    def get_diff_larger(self: "mrt_stats", mrt_s: "mrt_stats") -> "mrt_stats":
        """
        Generate an mrt_stats obj with entries which are unique to mrt_s, and
        are larger than the equivalent values in this obj. For example, only
        prefixes if the AS Path is longer, or only origin ASNs which sent more
        updates.
        """
        if not mrt_s:
            raise ValueError(f"Missing required options: mrt_s={mrt_s}")

        if type(mrt_s) != mrt_stats:
            raise TypeError(f"mrt_s is not a stats object: {type(mrt_s)}")

        diff = mrt_stats()
        diff.bogon_origin_asns = []
        diff.bogon_prefixes = []
        diff.highest_med_prefixes = []
        diff.invalid_len = []
        diff.longest_as_path = []
        diff.longest_comm_set = []
        diff.most_advt_prefixes = []
        diff.most_bogon_asns = []
        diff.most_upd_prefixes = []
        diff.most_withd_prefixes = []
        diff.most_advt_origin_asn = []
        diff.most_advt_peer_asn = []
        diff.most_upd_peer_asn = []
        diff.most_withd_peer_asn = []
        diff.most_origin_asns = []
        diff.most_unknown_attrs = []
        diff.most_unreg_origins = []

        updated = False

        # Prefixes with most bogon origin ASNs
        if mrt_s.bogon_origin_asns:
            if self.bogon_origin_asns:
                if (
                    mrt_s.bogon_origin_asns[0].prefix
                    and self.bogon_origin_asns[0].prefix
                ):
                    if len(mrt_s.bogon_origin_asns[0].origin_asns) > len(
                        self.bogon_origin_asns[0].origin_asns
                    ):
                        diff.bogon_origin_asns = mrt_s.bogon_origin_asns.copy()
                        updated = True
            else:
                diff.bogon_origin_asns = mrt_s.bogon_origin_asns.copy()
                updated = True

        # Bogons prefixes with more origin ASNs
        if mrt_s.bogon_prefixes:
            if self.bogon_prefixes:
                if (
                    mrt_s.bogon_prefixes[0].prefix
                    and self.bogon_prefixes[0].prefix
                ):
                    if len(mrt_s.bogon_prefixes[0].origin_asns) > len(
                        self.bogon_prefixes[0].origin_asns
                    ):
                        diff.bogon_prefixes = mrt_s.bogon_prefixes.copy()
                        updated = True
            else:
                diff.bogon_prefixes = mrt_s.bogon_prefixes.copy()
                updated = True

        # Highest MED
        if mrt_s.highest_med_prefixes:
            if self.highest_med_prefixes:
                if (
                    mrt_s.highest_med_prefixes[0].prefix
                    and self.highest_med_prefixes[0].prefix
                ):
                    if (
                        mrt_s.highest_med_prefixes[0].med
                        > self.highest_med_prefixes[0].med
                    ):
                        diff.highest_med_prefixes = (
                            mrt_s.highest_med_prefixes.copy()
                        )
                        updated = True
            else:
                diff.highest_med_prefixes = mrt_s.highest_med_prefixes.copy()
                updated = True

        # Invalid length prefixes with more origin ASNs
        if mrt_s.invalid_len:
            if self.invalid_len:
                if mrt_s.invalid_len[0].prefix and self.invalid_len[0].prefix:
                    if len(mrt_s.invalid_len[0].origin_asns) > len(
                        self.invalid_len[0].origin_asns
                    ):
                        diff.invalid_len = mrt_s.invalid_len.copy()
                        updated = True
            else:
                diff.invalid_len = mrt_s.invalid_len.copy()
                updated = True

        # Longer AS path
        if mrt_s.longest_as_path:
            if self.longest_as_path:
                if len(mrt_s.longest_as_path[0].as_path) > len(
                    self.longest_as_path[0].as_path
                ):
                    diff.longest_as_path = mrt_s.longest_as_path.copy()
                    updated = True
            else:
                diff.longest_as_path = mrt_s.longest_as_path.copy()
                updated = True

        # Longer community set
        if mrt_s.longest_comm_set:
            if self.longest_comm_set:
                if len(mrt_s.longest_comm_set[0].comm_set) > len(
                    self.longest_comm_set[0].comm_set
                ):
                    diff.longest_comm_set = mrt_s.longest_comm_set.copy()
                    updated = True
            else:
                diff.longest_comm_set = mrt_s.longest_comm_set.copy()
                updated = True

        # More advertisements per prefix
        # If stats from a rib dump are being compared this wont be present:
        if mrt_s.most_advt_prefixes:
            if self.most_advt_prefixes:
                if (
                    mrt_s.most_advt_prefixes[0].prefix
                    and self.most_advt_prefixes[0].prefix
                ):
                    if (
                        mrt_s.most_advt_prefixes[0].advt
                        > self.most_advt_prefixes[0].advt
                    ):
                        diff.most_advt_prefixes = (
                            mrt_s.most_advt_prefixes.copy()
                        )
                        updated = True
            else:
                diff.most_advt_prefixes = mrt_s.most_advt_prefixes.copy()
                updated = True

        # Origin ASNs with most bogon downstream origin ASNs
        if mrt_s.most_bogon_asns:
            if self.most_bogon_asns:
                if (
                    mrt_s.most_bogon_asns[0].as_path
                    and self.most_bogon_asns[0].as_path
                ):
                    if len(mrt_s.most_bogon_asns[0].origin_asns) > len(
                        self.most_bogon_asns[0].origin_asns
                    ):
                        diff.most_bogon_asns = mrt_s.most_bogon_asns.copy()
                        updated = True
            else:
                diff.most_bogon_asns = mrt_s.most_bogon_asns.copy()
                updated = True

        # More updates per prefix
        # If stats from a rib dump are being compared this wont be present:
        if mrt_s.most_upd_prefixes:
            if self.most_upd_prefixes:
                if (
                    mrt_s.most_upd_prefixes[0].prefix
                    and self.most_upd_prefixes[0].prefix
                ):
                    if (
                        mrt_s.most_upd_prefixes[0].updates
                        > self.most_upd_prefixes[0].updates
                    ):
                        diff.most_upd_prefixes = mrt_s.most_upd_prefixes.copy()
                        updated = True
            else:
                diff.most_upd_prefixes = mrt_s.most_upd_prefixes.copy()
                updated = True

        # More withdraws per prefix
        # If stats from a rib dump are being compared this wont be present:
        if mrt_s.most_withd_prefixes:
            if self.most_withd_prefixes:
                if (
                    mrt_s.most_withd_prefixes[0].prefix
                    and self.most_withd_prefixes[0].prefix
                ):
                    if (
                        mrt_s.most_withd_prefixes[0].withdraws
                        > self.most_withd_prefixes[0].withdraws
                    ):
                        diff.most_withd_prefixes = (
                            mrt_s.most_withd_prefixes.copy()
                        )
                        updated = True
            else:
                diff.most_withd_prefixes = mrt_s.most_withd_prefixes.copy()
                updated = True

        # More advertisement per origin ASN
        # If stats from a rib dump are being compare this wont be present:
        if mrt_s.most_advt_origin_asn:
            if self.most_advt_origin_asn:
                if (
                    mrt_s.most_advt_origin_asn[0].origin_asns
                    and self.most_advt_origin_asn[0].origin_asns
                ):
                    if (
                        mrt_s.most_advt_origin_asn[0].advt
                        > self.most_advt_origin_asn[0].advt
                    ):
                        diff.most_advt_origin_asn = (
                            mrt_s.most_advt_origin_asn.copy()
                        )
                        updated = True
            else:
                diff.most_advt_origin_asn = mrt_s.most_advt_origin_asn.copy()
                updated = True

        # More advertisement per peer ASN
        # If stats from a rib dump are being compared this wont be present:
        if mrt_s.most_advt_peer_asn:
            if self.most_advt_peer_asn:
                if (
                    mrt_s.most_advt_peer_asn[0].peer_asn
                    and self.most_advt_peer_asn[0].peer_asn
                ):
                    if (
                        mrt_s.most_advt_peer_asn[0].advt
                        > self.most_advt_peer_asn[0].advt
                    ):
                        diff.most_advt_peer_asn = (
                            mrt_s.most_advt_peer_asn.copy()
                        )
                        updated = True
            else:
                diff.most_advt_peer_asn = mrt_s.most_advt_peer_asn.copy()
                updated = True

        # More updates per peer ASN
        # If stats from a rib dump are being compared this wont be present:
        if mrt_s.most_upd_peer_asn:
            if self.most_upd_peer_asn:
                if (
                    mrt_s.most_upd_peer_asn[0].peer_asn
                    and self.most_upd_peer_asn[0].peer_asn
                ):
                    if (
                        mrt_s.most_upd_peer_asn[0].updates
                        > self.most_upd_peer_asn[0].updates
                    ):
                        diff.most_upd_peer_asn = mrt_s.most_upd_peer_asn.copy()
                        updated = True
            else:
                diff.most_upd_peer_asn = mrt_s.most_upd_peer_asn.copy()
                updated = True

        # More withdraws per peer ASN
        # If stats from a rib dump are being compared this wont be present:
        if mrt_s.most_withd_peer_asn:
            if self.most_withd_peer_asn:
                if (
                    mrt_s.most_withd_peer_asn[0].peer_asn
                    and self.most_withd_peer_asn[0].peer_asn
                ):
                    if (
                        mrt_s.most_withd_peer_asn[0].withdraws
                        > self.most_withd_peer_asn[0].withdraws
                    ):
                        diff.most_withd_peer_asn = (
                            mrt_s.most_withd_peer_asn.copy()
                        )
                        updated = True
            else:
                diff.most_withd_peer_asn = mrt_s.most_withd_peer_asn.copy()
                updated = True

        # More origin ASNs per prefix
        if mrt_s.most_origin_asns:
            if self.most_origin_asns:
                if (
                    mrt_s.most_origin_asns[0].prefix
                    and self.most_origin_asns[0].prefix
                ):
                    if len(mrt_s.most_origin_asns[0].origin_asns) > len(
                        self.most_origin_asns[0].origin_asns
                    ):
                        diff.most_origin_asns = mrt_s.most_origin_asns.copy()
                        updated = True
            else:
                diff.most_origin_asns = mrt_s.most_origin_asns.copy()
                updated = True

        # Prefixes with most unknown attributes
        if mrt_s.most_unknown_attrs:
            if self.most_unknown_attrs:
                if (
                    mrt_s.most_unknown_attrs[0].prefix
                    and self.most_unknown_attrs[0].prefix
                ):
                    if len(mrt_s.most_unknown_attrs[0].unknown_attrs) > len(
                        self.most_unknown_attrs[0].unknown_attrs
                    ):
                        diff.most_unknown_attrs = (
                            mrt_s.most_unknown_attrs.copy()
                        )
                        updated = True
            else:
                diff.most_unknown_attrs = mrt_s.most_unknown_attrs.copy()
                updated = True

        # Prefixes with most unregistered origin ASNs
        if mrt_s.most_unreg_origins:
            if self.most_unreg_origins:
                if (
                    mrt_s.most_unreg_origins[0].prefix
                    and self.most_unreg_origins[0].prefix
                ):
                    if len(mrt_s.most_unreg_origins[0].origin_asns) > len(
                        self.most_unreg_origins[0].origin_asns
                    ):
                        diff.most_unreg_origins = (
                            mrt_s.most_unreg_origins.copy()
                        )
                        updated = True
            else:
                diff.most_unreg_origins = mrt_s.most_unreg_origins.copy()
                updated = True

        # If stats from a rib dump are being compared, these wont be present:
        # More updates parsed
        if mrt_s.total_upd > self.total_upd:
            diff.total_upd = mrt_s.total_upd
            updated = True

        # More updates with advertisements
        if mrt_s.total_advt > self.total_advt:
            diff.total_advt = mrt_s.total_advt
            updated = True

        # More updates with withdraws
        if mrt_s.total_withd > self.total_withd:
            diff.total_withd = mrt_s.total_withd
            updated = True

        if updated:
            ### FIXME - this needs to an accumulating file list
            ###diff.file_list.extend(self.file_list)
            ###diff.file_list.extend(mrt_s.file_list)
            diff.timestamp = mrt_s.timestamp

        return diff

    @staticmethod
    def gen_prev_daily_key(ymd: str) -> str:
        """
        Generate the redis key used to store the global stats obj for the
        day before a specific day.
        """
        if not ymd:
            raise ValueError(f"Missing required arguments: ymd={ymd}")

        if type(ymd) != str:
            raise TypeError(f"ymd is not a string: {type(ymd)}")

        mrt_archive.valid_ymd(ymd)

        return "DAILY:" + datetime.datetime.strftime(
            datetime.datetime.strptime(ymd, cfg.DAY_FORMAT)
            - datetime.timedelta(days=1),
            cfg.DAY_FORMAT,
        )

    def is_empty(self: "mrt_stats") -> bool:
        """
        Check if an mrt_stats object is empty. Don't check meta data like
        file list or timestamp.
        """
        if (
            not self.bogon_origin_asns
            and not self.bogon_prefixes
            and not self.highest_med_prefixes
            and not self.invalid_len
            and not self.longest_as_path
            and not self.longest_comm_set
            and not self.most_advt_prefixes
            and not self.most_bogon_asns
            and not self.most_upd_prefixes
            and not self.most_withd_prefixes
            and not self.most_advt_origin_asn
            and not self.most_advt_peer_asn
            and not self.most_upd_peer_asn
            and not self.most_withd_peer_asn
            and not self.most_origin_asns
            and not self.most_unknown_attrs
            and not self.most_unreg_origins
            and not self.file_list
            and not self.timestamp
            and not self.total_upd
            and not self.total_advt
            and not self.total_withd
        ):
            return True
        else:
            return False

    def merge(self: "mrt_stats", merge_data: "mrt_stats") -> bool:
        """
        This functions takes the bigger stat from the local object and
        merge_data object, and stores the bigger of the two back in this object.

        This means that if merge_object has value which is equal to one in this
        object, that value will be appended to this one as a 2nd data point
        (they won't be added together). But if merge_data has a different value
        which is higher, smaller value in this object is replaced with the
        larger value.

        E.g, if both objects have the same "max updates per prefix" prefix,
        192.168.0.0/24, with both objects recording 1000 updates for this
        prefix, no change is made to this object. If merge_data has a
        different prefix, 192.168.1.0/24 also with 1000 updates, that will be
        appended to this object so that this object has two prefixes with 1000
        updates. If merge_data has a prefix with 10000 updates, 192.168.2.0/24,
        all prefixes in this object will be dropped, and this object will now
        contain 192.168.2.0/24 only.
        """
        if not merge_data:
            raise ValueError(
                f"Missing required options: merge_data={merge_data}"
            )

        if type(merge_data) != mrt_stats:
            raise TypeError(
                f"merge_data is not a stats object: {type(merge_data)}"
            )

        changed = False

        # Prefixes with most bogon origin ASNs
        if merge_data.bogon_origin_asns:
            if self.bogon_origin_asns:
                if (
                    len(merge_data.bogon_origin_asns[0].origin_asns)
                    == len(self.bogon_origin_asns[0].origin_asns)
                    and len(self.bogon_origin_asns[0].origin_asns) > 0
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.bogon_origin_asns
                    ]
                    for mrt_e in merge_data.bogon_origin_asns:
                        if mrt_e.prefix not in s_prefixes:
                            self.bogon_origin_asns.append(mrt_e)
                            changed = True
                elif len(merge_data.bogon_origin_asns[0].origin_asns) > len(
                    self.bogon_origin_asns[0].origin_asns
                ):
                    self.bogon_origin_asns = (
                        merge_data.bogon_origin_asns.copy()
                    )
                    changed = True
            else:
                self.bogon_origin_asns = merge_data.bogon_origin_asns.copy()
                changed = True

        # Bogons prefixes with most origin ASNs
        if merge_data.bogon_prefixes:
            if self.bogon_prefixes:
                if (
                    len(merge_data.bogon_prefixes[0].origin_asns)
                    == len(self.bogon_prefixes[0].origin_asns)
                    and len(self.bogon_prefixes[0].origin_asns) > 0
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.bogon_prefixes
                    ]
                    for mrt_e in merge_data.bogon_prefixes:
                        if mrt_e.prefix not in s_prefixes:
                            self.bogon_prefixes.append(mrt_e)
                            changed = True
                elif len(merge_data.bogon_prefixes[0].origin_asns) > len(
                    self.bogon_prefixes[0].origin_asns
                ):
                    self.bogon_prefixes = merge_data.bogon_prefixes.copy()
                    changed = True
            else:
                self.bogon_prefixes = merge_data.bogon_prefixes.copy()
                changed = True

        # Highest MED
        if merge_data.highest_med_prefixes:
            if self.highest_med_prefixes:
                if (
                    merge_data.highest_med_prefixes[0].med
                    == self.highest_med_prefixes[0].med
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.highest_med_prefixes
                    ]
                    med = self.highest_med_prefixes[0].med
                    for mrt_e in merge_data.highest_med_prefixes:
                        if mrt_e.prefix not in s_prefixes:
                            self.highest_med_prefixes.append(mrt_e)
                            changed = True
                elif (
                    merge_data.highest_med_prefixes[0].med
                    > self.highest_med_prefixes[0].med
                ):
                    self.highest_med_prefixes = (
                        merge_data.highest_med_prefixes.copy()
                    )
                    changed = True
            else:
                self.highest_med_prefixes = (
                    merge_data.highest_med_prefixes.copy()
                )
                changed = True

        # Invalid length prefixes with most origin ASNs
        if merge_data.invalid_len:
            if self.invalid_len:
                if (
                    len(merge_data.invalid_len[0].origin_asns)
                    == len(self.invalid_len[0].origin_asns)
                    and len(self.invalid_len[0].origin_asns) > 0
                ):
                    s_prefixes = [mrt_e.prefix for mrt_e in self.invalid_len]
                    for mrt_e in merge_data.invalid_len:
                        if mrt_e.prefix not in s_prefixes:
                            self.invalid_len.append(mrt_e)
                            changed = True
                elif len(merge_data.invalid_len[0].origin_asns) > len(
                    self.invalid_len[0].origin_asns
                ):
                    self.invalid_len = merge_data.invalid_len.copy()
                    changed = True
            else:
                self.invalid_len = merge_data.invalid_len.copy()
                changed = True

        # Longest AS path
        if merge_data.longest_as_path:
            if self.longest_as_path:
                if len(merge_data.longest_as_path[0].as_path) == len(
                    self.longest_as_path[0].as_path
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.longest_as_path
                    ]
                    s_paths = [mrt_e.as_path for mrt_e in self.longest_as_path]
                    for mrt_e in merge_data.longest_as_path:
                        if mrt_e.prefix in s_prefixes:
                            if mrt_e.as_path not in s_paths:
                                self.longest_as_path.append(mrt_e)
                                changed = True
                        else:
                            self.longest_as_path.append(mrt_e)
                            changed = True
                elif len(merge_data.longest_as_path[0].as_path) > len(
                    self.longest_as_path[0].as_path
                ):
                    self.longest_as_path = merge_data.longest_as_path.copy()
                    changed = True
            else:
                self.longest_as_path = merge_data.longest_as_path.copy()
                changed = True

        # Longest community set
        if merge_data.longest_comm_set:
            if self.longest_comm_set:
                if len(merge_data.longest_comm_set[0].comm_set) == len(
                    self.longest_comm_set[0].comm_set
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.longest_comm_set
                    ]
                    s_comms = [
                        mrt_e.comm_set for mrt_e in self.longest_comm_set
                    ]
                    for mrt_e in merge_data.longest_comm_set:
                        if mrt_e.prefix in s_prefixes:
                            if mrt_e.comm_set not in s_comms:
                                self.longest_comm_set.append(mrt_e)
                                changed = True
                        else:
                            self.longest_comm_set.append(mrt_e)
                            changed = True
                elif len(merge_data.longest_comm_set[0].comm_set) > len(
                    self.longest_comm_set[0].comm_set
                ):
                    self.longest_comm_set = merge_data.longest_comm_set.copy()
                    changed = True
            else:
                self.longest_comm_set = merge_data.longest_comm_set.copy()
                changed = True

        """
        Most advertisements per prefix
        If stats from a rib dump are being merged this wont be present:
        """
        if merge_data.most_advt_prefixes:
            if self.most_advt_prefixes:
                if (
                    merge_data.most_advt_prefixes[0].advt
                    == self.most_advt_prefixes[0].advt
                    and self.most_advt_prefixes[0].advt > 0
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.most_advt_prefixes
                    ]
                    for mrt_e in merge_data.most_advt_prefixes:
                        if mrt_e.prefix not in s_prefixes:
                            self.most_advt_prefixes.append(mrt_e)
                            changed = True
                elif (
                    merge_data.most_advt_prefixes[0].advt
                    > self.most_advt_prefixes[0].advt
                ):
                    self.most_advt_prefixes = (
                        merge_data.most_advt_prefixes.copy()
                    )
                    changed = True
            else:
                self.most_advt_prefixes = merge_data.most_advt_prefixes.copy()
                changed = True

        # Origin ASNs with most downstream bogon origin ASNs
        if merge_data.most_bogon_asns:
            if self.most_bogon_asns:
                if (
                    len(merge_data.most_bogon_asns[0].origin_asns)
                    == len(self.most_bogon_asns[0].origin_asns)
                    and len(self.most_bogon_asns[0].origin_asns) > 0
                ):
                    s_asns = [mrt_e.as_path for mrt_e in self.most_bogon_asns]
                    for mrt_e in merge_data.most_bogon_asns:
                        if mrt_e.as_path not in s_asns:
                            self.most_bogon_asns.append(mrt_e)
                            changed = True
                elif len(merge_data.most_bogon_asns[0].origin_asns) > len(
                    self.most_bogon_asns[0].origin_asns
                ):
                    self.most_bogon_asns = merge_data.most_bogon_asns.copy()
                    changed = True
            else:
                self.most_bogon_asns = merge_data.most_bogon_asns.copy()
                changed = True

        """
        Most updates per prefix
        If stats from a rib dump are being merged this wont be present:
        """
        if merge_data.most_upd_prefixes:
            if self.most_upd_prefixes:
                if (
                    merge_data.most_upd_prefixes[0].updates
                    == self.most_upd_prefixes[0].updates
                    and self.most_upd_prefixes[0].updates > 0
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.most_upd_prefixes
                    ]
                    for mrt_e in merge_data.most_upd_prefixes:
                        if mrt_e.prefix not in s_prefixes:
                            self.most_upd_prefixes.append(mrt_e)
                            changed = True
                elif (
                    merge_data.most_upd_prefixes[0].updates
                    > self.most_upd_prefixes[0].updates
                ):
                    self.most_upd_prefixes = (
                        merge_data.most_upd_prefixes.copy()
                    )
                    changed = True
            else:
                self.most_upd_prefixes = merge_data.most_upd_prefixes.copy()
                changed = True

        """
        Most withdraws per prefix
        If stats from a rib dump are being merged this wont be present:
        """
        if merge_data.most_withd_prefixes:
            if self.most_withd_prefixes:
                if (
                    merge_data.most_withd_prefixes[0].withdraws
                    == self.most_withd_prefixes[0].withdraws
                    and self.most_withd_prefixes[0].withdraws > 0
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.most_withd_prefixes
                    ]
                    for mrt_e in merge_data.most_withd_prefixes:
                        if mrt_e.prefix not in s_prefixes:
                            self.most_withd_prefixes.append(mrt_e)
                            changed = True
                elif (
                    merge_data.most_withd_prefixes[0].withdraws
                    > self.most_withd_prefixes[0].withdraws
                ):
                    self.most_withd_prefixes = (
                        merge_data.most_withd_prefixes.copy()
                    )
                    changed = True
            else:
                self.most_withd_prefixes = (
                    merge_data.most_withd_prefixes.copy()
                )
                changed = True

        """
        Most advertisement per origin ASN
        If stats from a rib dump are being merged this wont be present:
        """
        if merge_data.most_advt_origin_asn:
            if self.most_advt_origin_asn:
                if (
                    merge_data.most_advt_origin_asn[0].advt
                    == self.most_advt_origin_asn[0].advt
                    and self.most_advt_origin_asn[0].advt > 0
                ):
                    s_origin_asns = [
                        mrt_e.origin_asns
                        for mrt_e in self.most_advt_origin_asn
                    ]
                    for mrt_e in merge_data.most_advt_origin_asn:
                        if mrt_e.origin_asns not in s_origin_asns:
                            self.most_advt_origin_asn.append(mrt_e)
                            changed = True
                elif (
                    merge_data.most_advt_origin_asn[0].advt
                    > self.most_advt_origin_asn[0].advt
                ):
                    self.most_advt_origin_asn = (
                        merge_data.most_advt_origin_asn.copy()
                    )
                    changed = True
            else:
                self.most_advt_origin_asn = (
                    merge_data.most_advt_origin_asn.copy()
                )
                changed = True

        """
        Most advertisement per peer ASN
        If stats from a rib dump are being merged this wont be present:
        """
        if merge_data.most_advt_peer_asn:
            if self.most_advt_peer_asn:
                if (
                    merge_data.most_advt_peer_asn[0].advt
                    == self.most_advt_peer_asn[0].advt
                    and self.most_advt_peer_asn[0].advt > 0
                ):
                    s_peer_asns = [
                        mrt_e.peer_asn for mrt_e in self.most_advt_peer_asn
                    ]
                    for mrt_e in merge_data.most_advt_peer_asn:
                        if mrt_e.peer_asn not in s_peer_asns:
                            self.most_advt_peer_asn.append(mrt_e)
                            changed = True
                elif (
                    merge_data.most_advt_peer_asn[0].advt
                    > self.most_advt_peer_asn[0].advt
                ):
                    self.most_advt_peer_asn = (
                        merge_data.most_advt_peer_asn.copy()
                    )
                    changed = True
            else:
                self.most_advt_peer_asn = merge_data.most_advt_peer_asn.copy()
                changed = True

        """
        Most updates per peer ASN
        If stats from a rib dump are being merged this wont be present:
        """
        if merge_data.most_upd_peer_asn:
            if self.most_upd_peer_asn:
                if (
                    merge_data.most_upd_peer_asn[0].updates
                    == self.most_upd_peer_asn[0].updates
                    and self.most_upd_peer_asn[0].updates > 0
                ):
                    s_peer_asns = [
                        mrt_e.peer_asn for mrt_e in self.most_upd_peer_asn
                    ]
                    for mrt_e in merge_data.most_upd_peer_asn:
                        if mrt_e.peer_asn not in s_peer_asns:
                            self.most_upd_peer_asn.append(mrt_e)
                            changed = True
                elif (
                    merge_data.most_upd_peer_asn[0].updates
                    > self.most_upd_peer_asn[0].updates
                ):
                    self.most_upd_peer_asn = (
                        merge_data.most_upd_peer_asn.copy()
                    )
                    changed = True
            else:
                self.most_upd_peer_asn = merge_data.most_upd_peer_asn.copy()
                changed = True

        """
        Most withdraws per peer ASN
        If stats from a rib dump are being merged this wont be present:
        """
        if merge_data.most_withd_peer_asn:
            if self.most_withd_peer_asn:
                if (
                    merge_data.most_withd_peer_asn[0].withdraws
                    == self.most_withd_peer_asn[0].withdraws
                    and self.most_withd_peer_asn[0].withdraws > 0
                ):
                    s_peer_asns = [
                        mrt_e.peer_asn for mrt_e in self.most_withd_peer_asn
                    ]
                    for mrt_e in merge_data.most_withd_peer_asn:
                        if mrt_e.peer_asn not in s_peer_asns:
                            self.most_withd_peer_asn.append(mrt_e)
                            changed = True
                elif (
                    merge_data.most_withd_peer_asn[0].withdraws
                    > self.most_withd_peer_asn[0].withdraws
                ):
                    self.most_withd_peer_asn = (
                        merge_data.most_withd_peer_asn.copy()
                    )
                    changed = True
            else:
                self.most_withd_peer_asn = (
                    merge_data.most_withd_peer_asn.copy()
                )
                changed = True

        # Most origin ASNs per prefix
        if merge_data.most_origin_asns:
            if self.most_origin_asns:
                if (
                    len(merge_data.most_origin_asns[0].origin_asns)
                    == len(self.most_origin_asns[0].origin_asns)
                    and len(self.most_origin_asns[0].origin_asns) > 0
                ):
                    for mrt_e in merge_data.most_origin_asns:
                        for s_e in self.most_origin_asns:
                            if mrt_e.prefix == s_e.prefix:
                                if mrt_e.origin_asns != s_e.origin_asns:
                                    s_e.origin_asns = s_e.origin_asns.union(
                                        mrt_e.origin_asns
                                    )
                                    changed = True
                                break
                        else:
                            self.most_origin_asns.append(mrt_e)
                            changed = True
                elif len(merge_data.most_origin_asns[0].origin_asns) > len(
                    self.most_origin_asns[0].origin_asns
                ):
                    self.most_origin_asns = merge_data.most_origin_asns.copy()
                    changed = True
            else:
                self.most_origin_asns = merge_data.most_origin_asns.copy()
                changed = True

        # Prefixes with most unknown attributes
        if merge_data.most_unknown_attrs:
            if self.most_unknown_attrs:
                if (
                    len(merge_data.most_unknown_attrs[0].unknown_attrs)
                    == len(self.most_unknown_attrs[0].unknown_attrs)
                    and len(self.most_unknown_attrs[0].unknown_attrs) > 0
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.most_unknown_attrs
                    ]
                    for mrt_e in merge_data.most_unknown_attrs:
                        if mrt_e.prefix not in s_prefixes:
                            self.most_unknown_attrs.append(mrt_e)
                            changed = True
                elif len(merge_data.most_unknown_attrs[0].unknown_attrs) > len(
                    self.most_unknown_attrs[0].unknown_attrs
                ):
                    self.most_unknown_attrs = (
                        merge_data.most_unknown_attrs.copy()
                    )
                    changed = True
            else:
                self.most_unknown_attrs = merge_data.most_unknown_attrs.copy()
                changed = True

        # Prefixes with most unregistered origin ASNs
        if merge_data.most_unreg_origins:
            if self.most_unreg_origins:
                if (
                    len(merge_data.most_unreg_origins[0].origin_asns)
                    == len(self.most_unreg_origins[0].origin_asns)
                    and len(self.most_unreg_origins[0].origin_asns) > 0
                ):
                    s_prefixes = [
                        mrt_e.prefix for mrt_e in self.most_unreg_origins
                    ]
                    for mrt_e in merge_data.most_unreg_origins:
                        if mrt_e.prefix not in s_prefixes:
                            self.most_unreg_origins.append(mrt_e)
                            changed = True
                elif len(merge_data.most_unreg_origins[0].origin_asns) > len(
                    self.most_unreg_origins[0].origin_asns
                ):
                    self.most_unreg_origins = (
                        merge_data.most_unreg_origins.copy()
                    )
                    changed = True
            else:
                self.most_unreg_origins = merge_data.most_unreg_origins.copy()
                changed = True

        """
        Most updates parsed
        If stats from a rib dump are being merged, these wont be present:
        """
        if merge_data.total_upd:
            if merge_data.total_upd > self.total_upd:
                self.total_upd = merge_data.total_upd
                changed = True

        # Most updates announcing prefixes
        if merge_data.total_advt:
            if merge_data.total_advt > self.total_advt:
                self.total_advt = merge_data.total_advt
                changed = True

        # Most updates withdrawing prefixes
        if merge_data.total_withd:
            if merge_data.total_withd > self.total_withd:
                self.total_withd = merge_data.total_withd
                changed = True

        if changed:
            for filename in merge_data.file_list:
                if filename not in self.file_list:
                    self.file_list.append(filename)
            self.timestamp = merge_data.timestamp

        return changed

    def merge_archives(self: "mrt_stats", mrt_s: "mrt_stats"):
        """
        Add MRT archive names from mrt_s to this stats object, only if they
        are missing.
        """
        if not mrt_s:
            raise ValueError("mrt_s is required")
        self.archive_list.update(mrt_s.archive_list)

    def print(self: "mrt_stats") -> None:
        """
        Ugly print the stats in this obj.
        """
        for mrt_e in self.bogon_origin_asns:
            print(f"bogon_origin_asns->prefix: {mrt_e.prefix}")
            print(f"bogon_origin_asns->advt: {mrt_e.advt}")
            print(f"bogon_origin_asns->as_path: {mrt_e.as_path}")
            print(f"bogon_origin_asns->comm_set: {mrt_e.comm_set}")
            print(f"bogon_origin_asns->filename: {mrt_e.filename}")
            print(f"bogon_origin_asns->med: {mrt_e.med}")
            print(f"bogon_origin_asns->next_hop: {mrt_e.next_hop}")
            print(f"bogon_origin_asns->origin_asns: {mrt_e.origin_asns}")
            print(f"bogon_origin_asns->peer_asn: {mrt_e.peer_asn}")
            print(f"bogon_origin_asns->timestamp: {mrt_e.timestamp}")
            print(f"bogon_origin_asns->updates: {mrt_e.updates}")
            print(f"bogon_origin_asns->withdraws: {mrt_e.withdraws}")
            print(f"bogon_origin_asns->unknown_attrs: {mrt_e.unknown_attrs}")
        if self.bogon_origin_asns:
            print("")

        for mrt_e in self.bogon_prefixes:
            print(f"bogon_prefixes->prefix: {mrt_e.prefix}")
            print(f"bogon_prefixes->advt: {mrt_e.advt}")
            print(f"bogon_prefixes->as_path: {mrt_e.as_path}")
            print(f"bogon_prefixes->comm_set: {mrt_e.comm_set}")
            print(f"bogon_prefixes->filename: {mrt_e.filename}")
            print(f"bogon_prefixes->med: {mrt_e.med}")
            print(f"bogon_prefixes->next_hop: {mrt_e.next_hop}")
            print(f"bogon_prefixes->origin_asns: {mrt_e.origin_asns}")
            print(f"bogon_prefixes->peer_asn: {mrt_e.peer_asn}")
            print(f"bogon_prefixes->timestamp: {mrt_e.timestamp}")
            print(f"bogon_prefixes->updates: {mrt_e.updates}")
            print(f"bogon_prefixes->withdraws: {mrt_e.withdraws}")
            print(f"bogon_prefixes->unknown_attrs: {mrt_e.unknown_attrs}")
        if self.bogon_prefixes:
            print("")

        for mrt_e in self.highest_med_prefixes:
            print(f"highest_med_prefixes->prefix: {mrt_e.prefix}")
            print(f"highest_med_prefixes->advt: {mrt_e.advt}")
            print(f"highest_med_prefixes->as_path: {mrt_e.as_path}")
            print(f"highest_med_prefixes->comm_set: {mrt_e.comm_set}")
            print(f"highest_med_prefixes->filename: {mrt_e.filename}")
            print(f"highest_med_prefixes->med: {mrt_e.med}")
            print(f"highest_med_prefixes->next_hop: {mrt_e.next_hop}")
            print(f"highest_med_prefixes->origin_asns: {mrt_e.origin_asns}")
            print(f"highest_med_prefixes->peer_asn: {mrt_e.peer_asn}")
            print(f"highest_med_prefixes->timestamp: {mrt_e.timestamp}")
            print(f"highest_med_prefixes->updates: {mrt_e.updates}")
            print(f"highest_med_prefixes->withdraws: {mrt_e.withdraws}")
            print(
                f"highest_med_prefixes->unknown_attrs: {mrt_e.unknown_attrs}"
            )
        if self.invalid_len:
            print("")

        for mrt_e in self.invalid_len:
            print(f"invalid_len->prefix: {mrt_e.prefix}")
            print(f"invalid_len->advt: {mrt_e.advt}")
            print(f"invalid_len->as_path: {mrt_e.as_path}")
            print(f"invalid_len->comm_set: {mrt_e.comm_set}")
            print(f"invalid_len->filename: {mrt_e.filename}")
            print(f"invalid_len->med: {mrt_e.med}")
            print(f"invalid_len->next_hop: {mrt_e.next_hop}")
            print(f"invalid_len->origin_asns: {mrt_e.origin_asns}")
            print(f"invalid_len->peer_asn: {mrt_e.peer_asn}")
            print(f"invalid_len->timestamp: {mrt_e.timestamp}")
            print(f"invalid_len->updates: {mrt_e.updates}")
            print(f"invalid_len->withdraws: {mrt_e.withdraws}")
            print(f"invalid_len->unknown_attrs: {mrt_e.unknown_attrs}")
        if self.invalid_len:
            print("")

        for mrt_e in self.longest_as_path:
            print(f"longest_as_path->prefix: {mrt_e.prefix}")
            print(f"longest_as_path->advt: {mrt_e.advt}")
            print(f"longest_as_path->as_path: {mrt_e.as_path}")
            print(f"longest_as_path->comm_set: {mrt_e.comm_set}")
            print(f"longest_as_path->filename: {mrt_e.filename}")
            print(f"longest_as_path->med: {mrt_e.med}")
            print(f"longest_as_path->next_hop: {mrt_e.next_hop}")
            print(f"longest_as_path->origin_asns: {mrt_e.origin_asns}")
            print(f"longest_as_path->peer_asn: {mrt_e.peer_asn}")
            print(f"longest_as_path->timestamp: {mrt_e.timestamp}")
            print(f"longest_as_path->updates: {mrt_e.updates}")
            print(f"longest_as_path->withdraws: {mrt_e.withdraws}")
            print(f"longest_as_path->unknown_attrs: {mrt_e.unknown_attrs}")
        if self.longest_as_path:
            print("")

        for mrt_e in self.longest_comm_set:
            print(f"longest_comm_set->prefix: {mrt_e.prefix}")
            print(f"longest_comm_set->advt: {mrt_e.advt}")
            print(f"longest_comm_set->as_path: {mrt_e.as_path}")
            print(f"longest_comm_set->comm_set: {mrt_e.comm_set}")
            print(f"longest_comm_set->filename: {mrt_e.filename}")
            print(f"longest_comm_set->med: {mrt_e.med}")
            print(f"longest_comm_set->next_hop: {mrt_e.next_hop}")
            print(f"longest_comm_set->origin_asns: {mrt_e.origin_asns}")
            print(f"longest_comm_set->peer_asn: {mrt_e.peer_asn}")
            print(f"longest_comm_set->timestamp: {mrt_e.timestamp}")
            print(f"longest_comm_set->updates: {mrt_e.updates}")
            print(f"longest_comm_set->withdraws: {mrt_e.withdraws}")
            print(f"longest_comm_set->unknown_attrs: {mrt_e.unknown_attrs}")
        if self.longest_comm_set:
            print("")

        for mrt_e in self.most_advt_prefixes:
            print(f"most_advt_prefixes->prefix: {mrt_e.prefix}")
            print(f"most_advt_prefixes->advt: {mrt_e.advt}")
            print(f"most_advt_prefixes->as_path: {mrt_e.as_path}")
            print(f"most_advt_prefixes->comm_set: {mrt_e.comm_set}")
            print(f"most_advt_prefixes->filename: {mrt_e.filename}")
            print(f"most_advt_prefixes->med: {mrt_e.med}")
            print(f"most_advt_prefixes->next_hop: {mrt_e.next_hop}")
            print(f"most_advt_prefixes->origin_asns: {mrt_e.origin_asns}")
            print(f"most_advt_prefixes->peer_asn: {mrt_e.peer_asn}")
            print(f"most_advt_prefixes->timestamp: {mrt_e.timestamp}")
            print(f"most_advt_prefixes->updates: {mrt_e.updates}")
            print(f"most_advt_prefixes->withdraws: {mrt_e.withdraws}")
            print(f"most_advt_prefixes->unknown_attrs: {mrt_e.unknown_attrs}")
        if self.most_advt_prefixes:
            print("")

        for mrt_e in self.most_bogon_asns:
            print(f"most_bogon_asns->prefix: {mrt_e.prefix}")
            print(f"most_bogon_asns->advt: {mrt_e.advt}")
            print(f"most_bogon_asns->as_path: {mrt_e.as_path}")
            print(f"most_bogon_asns->comm_set: {mrt_e.comm_set}")
            print(f"most_bogon_asns->filename: {mrt_e.filename}")
            print(f"most_bogon_asns->med: {mrt_e.med}")
            print(f"most_bogon_asns->next_hop: {mrt_e.next_hop}")
            print(f"most_bogon_asns->origin_asns: {mrt_e.origin_asns}")
            print(f"most_bogon_asns->peer_asn: {mrt_e.peer_asn}")
            print(f"most_bogon_asns->timestamp: {mrt_e.timestamp}")
            print(f"most_bogon_asns->updates: {mrt_e.updates}")
            print(f"most_bogon_asns->withdraws: {mrt_e.withdraws}")
            print(f"most_bogon_asns->unknown_attrs: {mrt_e.unknown_attrs}")
        if self.most_bogon_asns:
            print("")

        for mrt_e in self.most_upd_prefixes:
            print(f"most_upd_prefixes->prefix: {mrt_e.prefix}")
            print(f"most_upd_prefixes->advt: {mrt_e.advt}")
            print(f"most_upd_prefixes->as_path: {mrt_e.as_path}")
            print(f"most_upd_prefixes->comm_set: {mrt_e.comm_set}")
            print(f"most_upd_prefixes->filename: {mrt_e.filename}")
            print(f"most_upd_prefixes->med: {mrt_e.med}")
            print(f"most_upd_prefixes->next_hop: {mrt_e.next_hop}")
            print(f"most_upd_prefixes->origin_asns: {mrt_e.origin_asns}")
            print(f"most_upd_prefixes->peer_asn: {mrt_e.peer_asn}")
            print(f"most_upd_prefixes->timestamp: {mrt_e.timestamp}")
            print(f"most_upd_prefixes->updates: {mrt_e.updates}")
            print(f"most_upd_prefixes->withdraws: {mrt_e.withdraws}")
            print(f"most_upd_prefixes->unknown_attrs: {mrt_e.unknown_attrs}")
        if self.most_upd_prefixes:
            print("")

        for mrt_e in self.most_withd_prefixes:
            print(f"most_withd_prefixes->prefix: {mrt_e.prefix}")
            print(f"most_withd_prefixes->advt: {mrt_e.advt}")
            print(f"most_withd_prefixes->as_path: {mrt_e.as_path}")
            print(f"most_withd_prefixes->comm_set: {mrt_e.comm_set}")
            print(f"most_withd_prefixes->filename: {mrt_e.filename}")
            print(f"most_withd_prefixes->med: {mrt_e.med}")
            print(f"most_withd_prefixes->next_hop: {mrt_e.next_hop}")
            print(f"most_withd_prefixes->origin_asns: {mrt_e.origin_asns}")
            print(f"most_withd_prefixes->peer_asn: {mrt_e.peer_asn}")
            print(f"most_withd_prefixes->timestamp: {mrt_e.timestamp}")
            print(f"most_withd_prefixes->updates: {mrt_e.updates}")
            print(f"most_withd_prefixes->withdraws: {mrt_e.withdraws}")
            print(f"most_withd_prefixes->unknown_attrs: {mrt_e.unknown_attrs}")
        if self.most_withd_prefixes:
            print("")

        for mrt_e in self.most_advt_origin_asn:
            print(f"most_advt_origin_asn->prefix: {mrt_e.prefix}")
            print(f"most_advt_origin_asn->advt: {mrt_e.advt}")
            print(f"most_advt_origin_asn->as_path: {mrt_e.as_path}")
            print(f"most_advt_origin_asn->comm_set: {mrt_e.comm_set}")
            print(f"most_advt_origin_asn->filename: {mrt_e.filename}")
            print(f"most_advt_origin_asn->med: {mrt_e.med}")
            print(f"most_advt_origin_asn->next_hop: {mrt_e.next_hop}")
            print(f"most_advt_origin_asn->origin_asns: {mrt_e.origin_asns}")
            print(f"most_advt_origin_asn->peer_asn: {mrt_e.peer_asn}")
            print(f"most_advt_origin_asn->timestamp: {mrt_e.timestamp}")
            print(f"most_advt_origin_asn->updates: {mrt_e.updates}")
            print(f"most_advt_origin_asn->withdraws: {mrt_e.withdraws}")
            print(
                f"most_advt_origin_asn->unknown_attrs: {mrt_e.unknown_attrs}"
            )
        if self.most_advt_origin_asn:
            print("")

        for mrt_e in self.most_advt_peer_asn:
            print(f"most_advt_peer_asn->prefix: {mrt_e.prefix}")
            print(f"most_advt_peer_asn->advt: {mrt_e.advt}")
            print(f"most_advt_peer_asn->as_path: {mrt_e.as_path}")
            print(f"most_advt_peer_asn->comm_set: {mrt_e.comm_set}")
            print(f"most_advt_peer_asn->filename: {mrt_e.filename}")
            print(f"most_advt_peer_asn->med: {mrt_e.med}")
            print(f"most_advt_peer_asn->next_hop: {mrt_e.next_hop}")
            print(f"most_advt_peer_asn->origin_asns: {mrt_e.origin_asns}")
            print(f"most_advt_peer_asn->peer_asn: {mrt_e.peer_asn}")
            print(f"most_advt_peer_asn->timestamp: {mrt_e.timestamp}")
            print(f"most_advt_peer_asn->updates: {mrt_e.updates}")
            print(f"most_advt_peer_asn->withdraws: {mrt_e.withdraws}")
            print(f"most_advt_peer_asn->unknown_attrs: {mrt_e.unknown_attrs}")
        if self.most_advt_peer_asn:
            print("")

        for mrt_e in self.most_upd_peer_asn:
            print(f"most_upd_peer_asn->prefix: {mrt_e.prefix}")
            print(f"most_upd_peer_asn->advt: {mrt_e.advt}")
            print(f"most_upd_peer_asn->as_path: {mrt_e.as_path}")
            print(f"most_upd_peer_asn->comm_set: {mrt_e.comm_set}")
            print(f"most_upd_peer_asn->filename: {mrt_e.filename}")
            print(f"most_upd_peer_asn->med: {mrt_e.med}")
            print(f"most_upd_peer_asn->next_hop: {mrt_e.next_hop}")
            print(f"most_upd_peer_asn->origin_asns: {mrt_e.origin_asns}")
            print(f"most_upd_peer_asn->peer_asn: {mrt_e.peer_asn}")
            print(f"most_upd_peer_asn->timestamp: {mrt_e.timestamp}")
            print(f"most_upd_peer_asn->updates: {mrt_e.updates}")
            print(f"most_upd_peer_asn->withdraws: {mrt_e.withdraws}")
            print(f"most_upd_peer_asn->unknown_attrs: {mrt_e.unknown_attrs}")
        if self.most_upd_peer_asn:
            print("")

        for mrt_e in self.most_withd_peer_asn:
            print(f"most_withd_peer_asn->prefix: {mrt_e.prefix}")
            print(f"most_withd_peer_asn->advt: {mrt_e.advt}")
            print(f"most_withd_peer_asn->as_path: {mrt_e.as_path}")
            print(f"most_withd_peer_asn->comm_set: {mrt_e.comm_set}")
            print(f"most_withd_peer_asn->filename: {mrt_e.filename}")
            print(f"most_withd_peer_asn->med: {mrt_e.med}")
            print(f"most_withd_peer_asn->next_hop: {mrt_e.next_hop}")
            print(f"most_withd_peer_asn->origin_asns: {mrt_e.origin_asns}")
            print(f"most_withd_peer_asn->peer_asn: {mrt_e.peer_asn}")
            print(f"most_withd_peer_asn->timestamp: {mrt_e.timestamp}")
            print(f"most_withd_peer_asn->updates: {mrt_e.updates}")
            print(f"most_withd_peer_asn->withdraws: {mrt_e.withdraws}")
            print(f"most_withd_peer_asn->unknown_attrs: {mrt_e.unknown_attrs}")
        if self.most_withd_peer_asn:
            print("")

        for mrt_e in self.most_origin_asns:
            print(f"most_origin_asns->prefix: {mrt_e.prefix}")
            print(f"most_origin_asns->advt: {mrt_e.advt}")
            print(f"most_origin_asns->as_path: {mrt_e.as_path}")
            print(f"most_origin_asns->comm_set: {mrt_e.comm_set}")
            print(f"most_origin_asns->filename: {mrt_e.filename}")
            print(f"most_origin_asns->med: {mrt_e.med}")
            print(f"most_origin_asns->next_hop: {mrt_e.next_hop}")
            print(f"most_origin_asns->origin_asns: {mrt_e.origin_asns}")
            print(f"most_origin_asns->peer_asn: {mrt_e.peer_asn}")
            print(f"most_origin_asns->timestamp: {mrt_e.timestamp}")
            print(f"most_origin_asns->updates: {mrt_e.updates}")
            print(f"most_origin_asns->withdraws: {mrt_e.withdraws}")
            print(f"most_origin_asns->unknown_attrs: {mrt_e.unknown_attrs}")
        if self.most_origin_asns:
            print("")

        for mrt_e in self.most_unknown_attrs:
            print(f"most_unknown_attrs->prefix: {mrt_e.prefix}")
            print(f"most_unknown_attrs->advt: {mrt_e.advt}")
            print(f"most_unknown_attrs->as_path: {mrt_e.as_path}")
            print(f"most_unknown_attrs->comm_set: {mrt_e.comm_set}")
            print(f"most_unknown_attrs->filename: {mrt_e.filename}")
            print(f"most_unknown_attrs->med: {mrt_e.med}")
            print(f"most_unknown_attrs->next_hop: {mrt_e.next_hop}")
            print(f"most_unknown_attrs->origin_asns: {mrt_e.origin_asns}")
            print(f"most_unknown_attrs->peer_asn: {mrt_e.peer_asn}")
            print(f"most_unknown_attrs->timestamp: {mrt_e.timestamp}")
            print(f"most_unknown_attrs->updates: {mrt_e.updates}")
            print(f"most_unknown_attrs->withdraws: {mrt_e.withdraws}")
            print(f"most_unknown_attrs->unknown_attrs: {mrt_e.unknown_attrs}")
        if self.most_unknown_attrs:
            print("")

        for mrt_e in self.most_unreg_origins:
            print(f"most_unreg_origins->prefix: {mrt_e.prefix}")
            print(f"most_unreg_origins->advt: {mrt_e.advt}")
            print(f"most_unreg_origins->as_path: {mrt_e.as_path}")
            print(f"most_unreg_origins->comm_set: {mrt_e.comm_set}")
            print(f"most_unreg_origins->filename: {mrt_e.filename}")
            print(f"most_unreg_origins->med: {mrt_e.med}")
            print(f"most_unreg_origins->next_hop: {mrt_e.next_hop}")
            print(f"most_unreg_origins->origin_asns: {mrt_e.origin_asns}")
            print(f"most_unreg_origins->peer_asn: {mrt_e.peer_asn}")
            print(f"most_unreg_origins->timestamp: {mrt_e.timestamp}")
            print(f"most_unreg_origins->updates: {mrt_e.updates}")
            print(f"most_unreg_origins->withdraws: {mrt_e.withdraws}")
            print(f"most_unreg_origins->unknown_attrs: {mrt_e.unknown_attrs}")
        if self.most_unreg_origins:
            print("")

        if self.total_upd:
            print(f"total_upd: {self.total_upd}")
        if self.total_advt:
            print(f"total_advt: {self.total_advt}")
        if self.total_withd:
            print(f"total_withd: {self.total_withd}")
        if self.file_list:
            print(f"file_list: {self.file_list}")
        if self.timestamp:
            print(f"timestamp: {self.timestamp}")

    def to_file(self: "mrt_stats", filename: str) -> None:
        """
        Serialise the MRT stats obj to JSON, save JSON as text file.
        """
        if not filename:
            raise ValueError(f"Missing required options: filename={filename}")

        if type(filename) != str:
            raise TypeError(f"filename is not a string: {type(filename)}")

        with open(filename, "w") as f:
            f.write(self.to_json())

    def to_json(
        self: "mrt_stats", indent: int = cfg.MRT_STATS_JSON_INDENT
    ) -> str:
        """
        Serialise the MRT stats obj to JSON, and returns the JSON string.
        """
        json_data = {
            "archive_list": list(self.archive_list),
            "bogon_origin_asns": [
                mrt_e.to_json() for mrt_e in self.bogon_origin_asns
            ],
            "bogon_prefixes": [
                mrt_e.to_json() for mrt_e in self.bogon_prefixes
            ],
            "highest_med_prefixes": [
                mrt_e.to_json() for mrt_e in self.highest_med_prefixes
            ],
            "invalid_len": [mrt_e.to_json() for mrt_e in self.invalid_len],
            "longest_as_path": [
                mrt_e.to_json() for mrt_e in self.longest_as_path
            ],
            "longest_comm_set": [
                mrt_e.to_json() for mrt_e in self.longest_comm_set
            ],
            "most_advt_prefixes": [
                mrt_e.to_json() for mrt_e in self.most_advt_prefixes
            ],
            "most_bogon_asns": [
                mrt_e.to_json() for mrt_e in self.most_bogon_asns
            ],
            "most_upd_prefixes": [
                mrt_e.to_json() for mrt_e in self.most_upd_prefixes
            ],
            "most_withd_prefixes": [
                mrt_e.to_json() for mrt_e in self.most_withd_prefixes
            ],
            "most_advt_origin_asn": [
                mrt_e.to_json() for mrt_e in self.most_advt_origin_asn
            ],
            "most_advt_peer_asn": [
                mrt_e.to_json() for mrt_e in self.most_advt_peer_asn
            ],
            "most_upd_peer_asn": [
                mrt_e.to_json() for mrt_e in self.most_upd_peer_asn
            ],
            "most_withd_peer_asn": [
                mrt_e.to_json() for mrt_e in self.most_withd_peer_asn
            ],
            "most_origin_asns": [
                mrt_e.to_json() for mrt_e in self.most_origin_asns
            ],
            "most_unknown_attrs": [
                mrt_e.to_json() for mrt_e in self.most_unknown_attrs
            ],
            "most_unreg_origins": [
                mrt_e.to_json() for mrt_e in self.most_unreg_origins
            ],
            "total_upd": self.total_upd,
            "total_advt": self.total_advt,
            "total_withd": self.total_withd,
            "file_list": self.file_list,
            "timestamp": self.timestamp,
        }
        return json.dumps(json_data, indent=indent)

    def ts_ymd(self: "mrt_stats") -> str:
        """
        Return only the YMD from this obj's timestamp raw e.g. YYYYMMDD
        """
        if not self.timestamp:
            raise ValueError(f"{self} has no timestamp: {self.timestamp}")

        return self.timestamp.split(".")[0]

    def ts_ymd_format(self: "mrt_stats") -> str:
        """
        Return only the YMD from this obj's timestamp formatted e.g. YYYY/MM/DD
        """
        if not self.timestamp:
            raise ValueError(f"{self} has no timestamp: {self.timestamp}")

        return (
            self.timestamp[0:4]
            + "/"
            + self.timestamp[4:6]
            + "/"
            + self.timestamp[6:8]
        )
