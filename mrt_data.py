from mrt_entry import mrt_entry

class mrt_data:

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


    def merge_chunks(self, merge_data):
        """
        Merge another mrt_data object into this one.
        """

        if len(merge_data.longest_as_path[0].as_path) == len(self.longest_as_path[0].as_path):
            s_paths = [mrt_e.as_path for mrt_e in self.longest_as_path]
            for mrt_e in merge_data.longest_as_path:
                if mrt_e.as_path not in s_paths:
                    self.longest_as_path.append(mrt_e)
        elif len(merge_data.longest_as_path[0].as_path) > len(self.longest_as_path[0].as_path):
            self.longest_as_path = merge_data.longest_as_path.copy()

        if len(merge_data.longest_community_set[0].community_set) == len(self.longest_community_set[0].community_set):
            s_comms = [mrt_e.community_set for mrt_e in self.longest_community_set]
            for mrt_e in merge_data.longest_community_set:
                if mrt_e.community_set not in s_comms:
                    self.longest_community_set.append(mrt_e)
        elif len(merge_data.longest_community_set[0].community_set) > len(self.longest_community_set[0].community_set):
            self.longest_community_set = merge_data.longest_community_set.copy()


        tmp = []
        for idx, u_e in enumerate(merge_data.most_advt_prefixes[:]):
            for res_e in self.most_advt_prefixes:
                if res_e.prefix == u_e.prefix:
                    tmp.append(
                        mrt_entry(
                            prefix=res_e.prefix,
                            advertisements=(res_e.advertisements + u_e.advertisements),
                        )
                    )
                    merge_data.most_advt_prefixes.remove(u_e)

        if tmp:
            for tmp_e in tmp:
                if tmp_e.advertisements == self.most_advt_prefixes[0].advertisements:
                    self.most_advt_prefixes.append(tmp_e)
                elif tmp_e.advertisements > self.most_advt_prefixes[0].advertisements:
                    self.most_advt_prefixes = [tmp_e]
        else:
            if (merge_data.most_advt_prefixes[0].advertisements == self.most_advt_prefixes[0].advertisements and
                self.most_advt_prefixes[0].advertisements > 0):
                self.most_advt_prefixes.extend(merge_data.most_advt_prefixes)
            elif merge_data.most_advt_prefixes[0].advertisements > self.most_advt_prefixes[0].advertisements:
                self.most_advt_prefixes = merge_data.most_advt_prefixes.copy()


        tmp = []
        for idx, u_e in enumerate(merge_data.most_upd_prefixes[:]):
            for res_e in self.most_upd_prefixes:
                if res_e.prefix == u_e.prefix:
                    tmp.append(
                        mrt_entry(
                            prefix=res_e.prefix,
                            updates=(res_e.updates + u_e.updates),
                        )
                    )
                    merge_data.most_upd_prefixes.remove(u_e)

        if tmp:
            for tmp_e in tmp:
                if tmp_e.updates == self.most_upd_prefixes[0].updates:
                    self.most_upd_prefixes.append(tmp_e)
                elif tmp_e.updates > self.most_upd_prefixes[0].updates:
                    self.most_upd_prefixes = [tmp_e]
        else:
            if (merge_data.most_upd_prefixes[0].updates == self.most_upd_prefixes[0].updates and
                self.most_upd_prefixes[0].updates > 0):
                self.most_upd_prefixes.extend(merge_data.most_upd_prefixes)
            elif merge_data.most_upd_prefixes[0].updates > self.most_upd_prefixes[0].updates:
                self.most_upd_prefixes = merge_data.most_upd_prefixes.copy()


        tmp = []
        for idx, u_e in enumerate(merge_data.most_withd_prefixes[:]):
            for res_e in self.most_withd_prefixes:
                if res_e.prefix == u_e.prefix:
                    tmp.append(
                        mrt_entry(
                            prefix=res_e.prefix,
                            withdraws=(res_e.withdraws + u_e.withdraws),
                        )
                    )
                    merge_data.most_withd_prefixes.remove(u_e)

        if tmp:
            for tmp_e in tmp:
                if tmp_e.withdraws == self.most_withd_prefixes[0].withdraws:
                    self.most_withd_prefixes.append(tmp_e)
                elif tmp_e.withdraws > self.most_withd_prefixes[0].withdraws:
                    self.most_withd_prefixes = [tmp_e]
        else:
            if (merge_data.most_withd_prefixes[0].withdraws == self.most_withd_prefixes[0].withdraws and
                self.most_withd_prefixes[0].withdraws > 0):
                self.most_withd_prefixes.extend(merge_data.most_withd_prefixes)
            elif merge_data.most_withd_prefixes[0].withdraws > self.most_withd_prefixes[0].withdraws:
                self.most_withd_prefixes = merge_data.most_withd_prefixes.copy()


        tmp = []
        for idx, u_e in enumerate(merge_data.most_advt_origin_asn[:]):
            for res_e in self.most_advt_origin_asn:
                if res_e.origin_asns == u_e.origin_asns:
                    tmp.append(
                        mrt_entry(
                            origin_asns=res_e.origin_asns,
                            advertisements=(res_e.advertisements + u_e.advertisements),
                        )
                    )
                    merge_data.most_advt_origin_asn.remove(u_e)

        if tmp:
            for tmp_e in tmp:
                if tmp_e.advertisements == self.most_advt_origin_asn[0].advertisements:
                    self.most_advt_origin_asn.append(tmp_e)
                elif tmp_e.advertisements > self.most_advt_origin_asn[0].advertisements:
                    self.most_advt_origin_asn = [tmp_e]
        else:
            if (merge_data.most_advt_origin_asn[0].advertisements == self.most_advt_origin_asn[0].advertisements and
                self.most_advt_origin_asn[0].advertisements > 0):
                self.most_advt_origin_asn.extend(merge_data.most_advt_origin_asn)
            elif merge_data.most_advt_origin_asn[0].advertisements > self.most_advt_origin_asn[0].advertisements:
                self.most_advt_origin_asn = merge_data.most_advt_origin_asn.copy()


        tmp = []
        for idx, u_e in enumerate(merge_data.most_advt_peer_asn[:]):
            for res_e in self.most_advt_peer_asn:
                if res_e.peer_asn == u_e.peer_asn:
                    tmp.append(
                        mrt_entry(
                            peer_asn=res_e.peer_asn,
                            advertisements=(res_e.advertisements + u_e.advertisements),
                        )
                    )
                    merge_data.most_advt_peer_asn.remove(u_e)

        if tmp:
            for tmp_e in tmp:
                if tmp_e.advertisements == self.most_advt_peer_asn[0].advertisements:
                    self.most_advt_peer_asn.append(tmp_e)
                elif tmp_e.advertisements > self.most_advt_peer_asn[0].advertisements:
                    self.most_advt_peer_asn = [tmp_e]
        else:
            if (merge_data.most_advt_peer_asn[0].advertisements == self.most_advt_peer_asn[0].advertisements and
                self.most_advt_peer_asn[0].advertisements > 0):
                self.most_advt_peer_asn.extend(merge_data.most_advt_peer_asn)
            elif merge_data.most_advt_peer_asn[0].advertisements > self.most_advt_peer_asn[0].advertisements:
                self.most_advt_peer_asn = merge_data.most_advt_peer_asn.copy()


        tmp = []
        for idx, u_e in enumerate(merge_data.most_upd_peer_asn[:]):
            for res_e in self.most_upd_peer_asn:
                if res_e.peer_asn == u_e.peer_asn:
                    tmp.append(
                        mrt_entry(
                            peer_asn=res_e.peer_asn,
                            updates=(res_e.updates + u_e.updates),
                        )
                    )
                    merge_data.most_upd_peer_asn.remove(u_e)

        if tmp:
            for tmp_e in tmp:
                if tmp_e.updates == self.most_upd_peer_asn[0].updates:
                    self.most_upd_peer_asn.append(tmp_e)
                elif tmp_e.updates > self.most_upd_peer_asn[0].updates:
                    self.most_upd_peer_asn = [tmp_e]
        else:
            if (merge_data.most_upd_peer_asn[0].updates == self.most_upd_peer_asn[0].updates and
                self.most_upd_peer_asn[0].updates > 0):
                self.most_upd_peer_asn.extend(merge_data.most_upd_peer_asn)
            elif merge_data.most_upd_peer_asn[0].updates > self.most_upd_peer_asn[0].updates:
                self.most_upd_peer_asn = merge_data.most_upd_peer_asn.copy()

        tmp = []
        for idx, u_e in enumerate(merge_data.most_withd_peer_asn[:]):
            for res_e in self.most_withd_peer_asn:
                if res_e.peer_asn == u_e.peer_asn:
                    tmp.append(
                        mrt_entry(
                            peer_asn=res_e.peer_asn,
                            withdraws=(res_e.withdraws + u_e.withdraws),
                        )
                    )
                    merge_data.most_withd_peer_asn.remove(u_e)  ############### DO WE NEED TO REMOVE - merge_data NOT USED if tmp
                    ###### IF WE HAVE THIS - DO WE NEED AN "if" to check if merge_data is not empty, under the "else" below?

        if tmp:
            for tmp_e in tmp:
                if tmp_e.withdraws == self.most_withd_peer_asn[0].withdraws:
                    self.most_withd_peer_asn.append(tmp_e)
                elif tmp_e.withdraws > self.most_withd_peer_asn[0].withdraws:
                    self.most_withd_peer_asn = [tmp_e]
        else:
            if (merge_data.most_withd_peer_asn[0].withdraws == self.most_withd_peer_asn[0].withdraws and
                self.most_withd_peer_asn[0].withdraws > 0):
                self.most_withd_peer_asn.extend(merge_data.most_withd_peer_asn)
            elif merge_data.most_withd_peer_asn[0].withdraws > self.most_withd_peer_asn[0].withdraws:
                self.most_withd_peer_asn = merge_data.most_withd_peer_asn.copy()


        tmp = []
        for idx, u_e in enumerate(merge_data.most_origin_asns[:]):
            for res_e in self.most_origin_asns:
                if (res_e.prefix == u_e.prefix and
                    res_e.origin_asns != u_e.origin_asns):
                    tmp.append(
                        mrt_entry(
                            prefix=res_e.prefix,
                            origin_asns=res_e.origin_asns.union(u_e.origin_asns),
                        )
                    )
                    print(f"update prefix: {u_e.prefix}")
                    print(f"update origin_asns: {u_e.origin_asns}")
                    print(f"self prefix: {res_e.prefix}")
                    print(f"self origin_asns: {res_e.origin_asns}")
                    print(f"Merged to {tmp[-1].origin_asns}")
                    merge_data.most_origin_asns.remove(u_e) ############### DO WE NEED TO REMOVE - merge_data NOT USED if tmp

                elif (res_e.prefix == u_e.prefix and
                    res_e.origin_asns == u_e.origin_asns):
                    merge_data.most_origin_asns.remove(u_e)

        if tmp:
            for tmp_e in tmp:
                if len(tmp_e.origin_asns) == len(self.most_origin_asns[0].origin_asns):
                    self.most_origin_asns.append(tmp_e)
                elif len(tmp_e.origin_asns) > len(self.most_origin_asns[0].origin_asns):
                    self.most_origin_asns = [tmp_e]
        else:
            if merge_data.most_origin_asns:
                if len(merge_data.most_origin_asns[0].origin_asns) == len(self.most_origin_asns[0].origin_asns):
                    self.most_origin_asns.extend(merge_data.most_origin_asns)
                elif len(merge_data.most_origin_asns[0].origin_asns) > len(self.most_origin_asns[0].origin_asns):
                    self.most_origin_asns = merge_data.most_origin_asns.copy()
