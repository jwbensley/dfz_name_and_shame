import json
from dnas.mrt_entry import mrt_entry
from dnas.mrt_archive import mrt_archive

class mrt_stats:

    def __init__(self):
        self.longest_as_path = [mrt_entry()]
        self.longest_comm_set = [mrt_entry()]
        self.most_advt_prefixes = [mrt_entry()]
        self.most_upd_prefixes = [mrt_entry()]
        self.most_withd_prefixes = [mrt_entry()]
        self.most_advt_origin_asn = [mrt_entry()]
        self.most_advt_peer_asn = [mrt_entry()]
        self.most_upd_peer_asn = [mrt_entry()]
        self.most_withd_peer_asn = [mrt_entry()]
        self.most_origin_asns = [mrt_entry()]
        self.file_list = []
        self.timestamp = None

    def equal_to(self, mrt_s):
        """
        Return True if this MRT stats obj is the same as mrt_s, else False.
        Don't compare meta data like file list or timestamp, on the stats.
        """

        if len(self.longest_as_path) != len(mrt_s.longest_as_path):
            return False

        for self_e in self.longest_as_path:
            for mrt_e in mrt_s.longest_as_path[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.longest_as_path.remove(mrt_e)
        if mrt_s.longest_as_path:
            return False


        if len(self.longest_comm_set) != len(mrt_s.longest_comm_set):
            return False

        for self_e in self.longest_comm_set:
            for mrt_e in mrt_s.longest_comm_set[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.longest_comm_set.remove(mrt_e)
        if mrt_s.longest_comm_set:
            return False


        if len(self.most_advt_prefixes) != len(mrt_s.most_advt_prefixes):
            return False

        for self_e in self.most_advt_prefixes:
            for mrt_e in mrt_s.most_advt_prefixes[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_advt_prefixes.remove(mrt_e)
        if mrt_s.most_advt_prefixes:
            return False


        if len(self.most_upd_prefixes) != len(mrt_s.most_upd_prefixes):
            return False

        for self_e in self.most_upd_prefixes:
            for mrt_e in mrt_s.most_upd_prefixes[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_upd_prefixes.remove(mrt_e)
        if mrt_s.most_upd_prefixes:
            return False


        if len(self.most_withd_prefixes) != len(mrt_s.most_withd_prefixes):
            return False

        for self_e in self.most_withd_prefixes:
            for mrt_e in mrt_s.most_withd_prefixes[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_withd_prefixes.remove(mrt_e)
        if mrt_s.most_withd_prefixes:
            return False


        if len(self.most_advt_origin_asn) != len(mrt_s.most_advt_origin_asn):
            return False

        for self_e in self.most_advt_origin_asn:
            for mrt_e in mrt_s.most_advt_origin_asn[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_advt_origin_asn.remove(mrt_e)
        if mrt_s.most_advt_origin_asn:
            return False


        if len(self.most_advt_peer_asn) != len(mrt_s.most_advt_peer_asn):
            return False

        for self_e in self.most_advt_peer_asn:
            for mrt_e in mrt_s.most_advt_peer_asn[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_advt_peer_asn.remove(mrt_e)
        if mrt_s.most_advt_peer_asn:
            return False


        if len(self.most_upd_peer_asn) != len(mrt_s.most_upd_peer_asn):
            return False

        for self_e in self.most_upd_peer_asn:
            for mrt_e in mrt_s.most_upd_peer_asn[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_upd_peer_asn.remove(mrt_e)
        if mrt_s.most_upd_peer_asn:
            return False


        if len(self.most_withd_peer_asn) != len(mrt_s.most_withd_peer_asn):
            return False

        for self_e in self.most_withd_peer_asn:
            for mrt_e in mrt_s.most_withd_peer_asn[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_withd_peer_asn.remove(mrt_e)
        if mrt_s.most_withd_peer_asn:
            return False


        if len(self.most_origin_asns) != len(mrt_s.most_origin_asns):
            return False

        for self_e in self.most_origin_asns:
            for mrt_e in mrt_s.most_origin_asns[:]:
                if self_e.equal_to(mrt_e):
                    mrt_s.most_origin_asns.remove(mrt_e)
        if mrt_s.most_origin_asns:
            return False

        return True

    def from_file(self, filename):
        """
        Load and parse MRT stats obj from a JSON text file.
        """
        with open(filename, "r") as f:
            self.from_json(f.read())

    def from_json(self, json_str):
        """
        Parse the JSON string as MRT stats data.
        """
        json_dict = json.loads(json_str)

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

        self.file_list = json_dict["file_list"]

        self.timestamp = json_dict["timestamp"]

    @staticmethod
    def gen_daily_key(ymd):
        """
        Generate the redis key used to store the global stats obj for a
        specific day.
        """
        if not ymd:
            raise ValueError(
                f"Missing required arguments: ymd={ymd}"
            )

        mrt_archive.valid_ymd(ymd)

        return "DAILY:" + ymd

    @staticmethod
    def gen_global_key():
        """
        Generate the key used to store the running global stats obj in redis.
        """
        return "GLOBAL"

    def get_diff(self, mrt_s):
        """
        Generate an mrt_stats obj with entries unique to mrt_s.
        Don't diff meta data like timestamp or file list.
        """
        diff = mrt_stats()
        diff.longest_as_path = []
        diff.longest_comm_set = []
        diff.most_advt_prefixes = []
        diff.most_upd_prefixes = []
        diff.most_withd_prefixes = []
        diff.most_advt_origin_asn = []
        diff.most_advt_peer_asn = []
        diff.most_upd_peer_asn = []
        diff.most_withd_peer_asn = []
        diff.most_origin_asns = []

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

        return diff

    @staticmethod
    def is_empty(mrt_s):
        """
        Check if an mrt_stats object is empty. Don't check meta data like
        file list or timestamp.
        """
        if (not diff.longest_as_path and
            not diff.longest_comm_set and
            not diff.most_advt_prefixes and
            not diff.most_upd_prefixes and
            not diff.most_withd_prefixes and
            not diff.most_advt_origin_asn and
            not diff.most_advt_peer_asn and
            not diff.most_upd_peer_asn and
            not diff.most_withd_peer_asn and
            not diff.most_origin_asns
        ):
            return True
        else:
            return False

    def merge_in(self, merge_data):
        """
        Merge another MRT stats object into this one.
        Only stats data is merged, not meta data like timestamp or file list.
        """

        changed = False

        if len(merge_data.longest_as_path[0].as_path) == len(self.longest_as_path[0].as_path):
            s_prefixes = [mrt_e.prefix for mrt_e in self.longest_as_path]
            s_paths = [mrt_e.as_path for mrt_e in self.longest_as_path]
            for mrt_e in merge_data.longest_as_path:
                if mrt_e.as_path not in s_paths:
                    if mrt_e.prefix not in s_prefixes:
                        self.longest_as_path.append(mrt_e)
                        changed = True
        elif len(merge_data.longest_as_path[0].as_path) > len(self.longest_as_path[0].as_path):
            self.longest_as_path = merge_data.longest_as_path.copy()
            changed = True

        if len(merge_data.longest_comm_set[0].comm_set) == len(self.longest_comm_set[0].comm_set):
            s_prefixes = [mrt_e.prefix for mrt_e in self.longest_comm_set]
            s_comms = [mrt_e.comm_set for mrt_e in self.longest_comm_set]
            for mrt_e in merge_data.longest_comm_set:
                if mrt_e.comm_set not in s_comms:
                    if mrt_e.prefix not in s_prefixes:
                        self.longest_comm_set.append(mrt_e)
                        changed = True
        elif len(merge_data.longest_comm_set[0].comm_set) > len(self.longest_comm_set[0].comm_set):
            self.longest_comm_set = merge_data.longest_comm_set.copy()
            changed = True


        tmp = []
        # In case a rib dump is being merged, this stat wont be present
        if merge_data.most_advt_prefixes[0].prefix:
            for idx, u_e in enumerate(merge_data.most_advt_prefixes[:]):
                for res_e in self.most_advt_prefixes:
                    if (res_e.prefix == u_e.prefix):
                        tmp.append(
                            mrt_entry(
                                prefix=res_e.prefix,
                                advt=(res_e.advt + u_e.advt),
                            )
                        )
                        merge_data.most_advt_prefixes.remove(u_e) ############### DO WE NEED TO REMOVE - merge_data NOT USED if tmp

        if tmp:
            for tmp_e in tmp:
                if tmp_e.advt == self.most_advt_prefixes[0].advt:
                    self.most_advt_prefixes.append(tmp_e)
                    changed = True
                elif tmp_e.advt > self.most_advt_prefixes[0].advt:
                    self.most_advt_prefixes = [tmp_e]
                    changed = True
        else:
            if (merge_data.most_advt_prefixes[0].advt == self.most_advt_prefixes[0].advt and
                self.most_advt_prefixes[0].advt > 0):
                self.most_advt_prefixes.extend(merge_data.most_advt_prefixes)
                changed = True
            elif merge_data.most_advt_prefixes[0].advt > self.most_advt_prefixes[0].advt:
                self.most_advt_prefixes = merge_data.most_advt_prefixes.copy()
                changed = True


        tmp = []
        # In case a rib dump is being merged, this stat wont be present
        if merge_data.most_upd_prefixes[0].prefix:
            for idx, u_e in enumerate(merge_data.most_upd_prefixes[:]):
                for res_e in self.most_upd_prefixes:
                    if res_e.prefix == u_e.prefix:
                        tmp.append(
                            mrt_entry(
                                prefix=res_e.prefix,
                                updates=(res_e.updates + u_e.updates),
                            )
                        )
                        merge_data.most_upd_prefixes.remove(u_e) ############### DO WE NEED TO REMOVE - merge_data NOT USED if tmp

        if tmp:
            for tmp_e in tmp:
                if tmp_e.updates == self.most_upd_prefixes[0].updates:
                    self.most_upd_prefixes.append(tmp_e)
                    changed = True
                elif tmp_e.updates > self.most_upd_prefixes[0].updates:
                    self.most_upd_prefixes = [tmp_e]
                    changed = True
        else:
            if (merge_data.most_upd_prefixes[0].updates == self.most_upd_prefixes[0].updates and
                self.most_upd_prefixes[0].updates > 0):
                self.most_upd_prefixes.extend(merge_data.most_upd_prefixes)
                changed = True
            elif merge_data.most_upd_prefixes[0].updates > self.most_upd_prefixes[0].updates:
                self.most_upd_prefixes = merge_data.most_upd_prefixes.copy()
                changed = True


        tmp = []
        # In case a rib dump is being merged, this stat wont be present
        if merge_data.most_withd_prefixes[0].prefix:
            for idx, u_e in enumerate(merge_data.most_withd_prefixes[:]):
                for res_e in self.most_withd_prefixes:
                    if res_e.prefix == u_e.prefix:
                        tmp.append(
                            mrt_entry(
                                prefix=res_e.prefix,
                                withdraws=(res_e.withdraws + u_e.withdraws),
                            )
                        )
                        merge_data.most_withd_prefixes.remove(u_e) ############### DO WE NEED TO REMOVE - merge_data NOT USED if tmp

        if tmp:
            for tmp_e in tmp:
                if tmp_e.withdraws == self.most_withd_prefixes[0].withdraws:
                    self.most_withd_prefixes.append(tmp_e)
                    changed = True
                elif tmp_e.withdraws > self.most_withd_prefixes[0].withdraws:
                    self.most_withd_prefixes = [tmp_e]
                    changed = True
        else:
            if (merge_data.most_withd_prefixes[0].withdraws == self.most_withd_prefixes[0].withdraws and
                self.most_withd_prefixes[0].withdraws > 0):
                self.most_withd_prefixes.extend(merge_data.most_withd_prefixes)
                changed = True
            elif merge_data.most_withd_prefixes[0].withdraws > self.most_withd_prefixes[0].withdraws:
                self.most_withd_prefixes = merge_data.most_withd_prefixes.copy()
                changed = True


        tmp = []
        # In case a rib dump is being merged, this stat wont be present
        if merge_data.most_advt_origin_asn[0].prefix:
            for idx, u_e in enumerate(merge_data.most_advt_origin_asn[:]):
                for res_e in self.most_advt_origin_asn:
                    if res_e.origin_asns == u_e.origin_asns:
                        tmp.append(
                            mrt_entry(
                                origin_asns=res_e.origin_asns,
                                advt=(res_e.advt + u_e.advt),
                            )
                        )
                        merge_data.most_advt_origin_asn.remove(u_e) ############### DO WE NEED TO REMOVE - merge_data NOT USED if tmp

        if tmp:
            for tmp_e in tmp:
                if tmp_e.advt == self.most_advt_origin_asn[0].advt:
                    self.most_advt_origin_asn.append(tmp_e)
                    changed = True
                elif tmp_e.advt > self.most_advt_origin_asn[0].advt:
                    self.most_advt_origin_asn = [tmp_e]
                    changed = True
        else:
            if (merge_data.most_advt_origin_asn[0].advt == self.most_advt_origin_asn[0].advt and
                self.most_advt_origin_asn[0].advt > 0):
                self.most_advt_origin_asn.extend(merge_data.most_advt_origin_asn)
                changed = True
            elif merge_data.most_advt_origin_asn[0].advt > self.most_advt_origin_asn[0].advt:
                self.most_advt_origin_asn = merge_data.most_advt_origin_asn.copy()
                changed = True


        tmp = []
        # In case a rib dump is being merged, this stat wont be present
        if merge_data.most_advt_peer_asn[0].prefix:
            for idx, u_e in enumerate(merge_data.most_advt_peer_asn[:]):
                for res_e in self.most_advt_peer_asn:
                    if res_e.peer_asn == u_e.peer_asn:
                        tmp.append(
                            mrt_entry(
                                peer_asn=res_e.peer_asn,
                                advt=(res_e.advt + u_e.advt),
                            )
                        )
                        merge_data.most_advt_peer_asn.remove(u_e) ############### DO WE NEED TO REMOVE - merge_data NOT USED if tmp

        if tmp:
            for tmp_e in tmp:
                if tmp_e.advt == self.most_advt_peer_asn[0].advt:
                    self.most_advt_peer_asn.append(tmp_e)
                    changed = True
                elif tmp_e.advt > self.most_advt_peer_asn[0].advt:
                    self.most_advt_peer_asn = [tmp_e]
                    changed = True
        else:
            if (merge_data.most_advt_peer_asn[0].advt == self.most_advt_peer_asn[0].advt and
                self.most_advt_peer_asn[0].advt > 0):
                self.most_advt_peer_asn.extend(merge_data.most_advt_peer_asn)
                changed = True
            elif merge_data.most_advt_peer_asn[0].advt > self.most_advt_peer_asn[0].advt:
                self.most_advt_peer_asn = merge_data.most_advt_peer_asn.copy()
                changed = True


        tmp = []
        # In case a rib dump is being merged, this stat wont be present
        if merge_data.most_upd_peer_asn[0].prefix:
            for idx, u_e in enumerate(merge_data.most_upd_peer_asn[:]):
                for res_e in self.most_upd_peer_asn:
                    if res_e.peer_asn == u_e.peer_asn:
                        tmp.append(
                            mrt_entry(
                                peer_asn=res_e.peer_asn,
                                updates=(res_e.updates + u_e.updates),
                            )
                        )
                        merge_data.most_upd_peer_asn.remove(u_e) ############### DO WE NEED TO REMOVE - merge_data NOT USED if tmp

        if tmp:
            for tmp_e in tmp:
                if tmp_e.updates == self.most_upd_peer_asn[0].updates:
                    self.most_upd_peer_asn.append(tmp_e)
                    changed = True
                elif tmp_e.updates > self.most_upd_peer_asn[0].updates:
                    self.most_upd_peer_asn = [tmp_e]
                    changed = True
        else:
            if (merge_data.most_upd_peer_asn[0].updates == self.most_upd_peer_asn[0].updates and
                self.most_upd_peer_asn[0].updates > 0):
                self.most_upd_peer_asn.extend(merge_data.most_upd_peer_asn)
                changed = True
            elif merge_data.most_upd_peer_asn[0].updates > self.most_upd_peer_asn[0].updates:
                self.most_upd_peer_asn = merge_data.most_upd_peer_asn.copy()
                changed = True

        tmp = []
        # In case a rib dump is being merged, this stat wont be present
        if merge_data.most_withd_peer_asn[0].prefix:
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
                    changed = True
                elif tmp_e.withdraws > self.most_withd_peer_asn[0].withdraws:
                    self.most_withd_peer_asn = [tmp_e]
                    changed = True
        else:
            if (merge_data.most_withd_peer_asn[0].withdraws == self.most_withd_peer_asn[0].withdraws and
                self.most_withd_peer_asn[0].withdraws > 0):
                self.most_withd_peer_asn.extend(merge_data.most_withd_peer_asn)
                changed = True
            elif merge_data.most_withd_peer_asn[0].withdraws > self.most_withd_peer_asn[0].withdraws:
                self.most_withd_peer_asn = merge_data.most_withd_peer_asn.copy()
                changed = True


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
                    ####print(f"update prefix: {u_e.prefix}")
                    ####print(f"update origin_asns: {u_e.origin_asns}")
                    ####print(f"self prefix: {res_e.prefix}")
                    ####print(f"self origin_asns: {res_e.origin_asns}")
                    ####print(f"Merged to {tmp[-1].origin_asns}")
                    #####merge_data.most_origin_asns.remove(u_e) ############### DO WE NEED TO REMOVE - merge_data NOT USED if tmp

                ###elif (res_e.prefix == u_e.prefix and
                ###    res_e.origin_asns == u_e.origin_asns):
                ###    merge_data.most_origin_asns.remove(u_e) ############### DO WE NEED TO REMOVE - merge_data NOT USED if tmp

        if tmp:
            for tmp_e in tmp:
                if len(tmp_e.origin_asns) == len(self.most_origin_asns[0].origin_asns):
                    self.most_origin_asns.append(tmp_e)
                    changed = True
                elif len(tmp_e.origin_asns) > len(self.most_origin_asns[0].origin_asns):
                    self.most_origin_asns = [tmp_e]
                    changed = True
        else:
            if merge_data.most_origin_asns:
                if (
                    len(merge_data.most_origin_asns[0].origin_asns) == len(self.most_origin_asns[0].origin_asns) and
                    len(self.most_origin_asns[0].origin_asns) > 0
                ):
                    self.most_origin_asns.extend(merge_data.most_origin_asns)

                    """
                    self.most_advt_origin_asn[0].advt > 0):
                    s_prefixes = [mrt_e.prefix for mrt_e in self.most_advt_origin_asn]
                    for mrt_e in merge_data.most_advt_origin_asn:
                            if mrt_e.prefix not in s_prefixes:
                                self.most_advt_origin_asn.append(mrt_e)
                                changed = True

                    changed = True
                    """
                elif len(merge_data.most_origin_asns[0].origin_asns) > len(self.most_origin_asns[0].origin_asns):
                    self.most_origin_asns = merge_data.most_origin_asns.copy()
                    changed = True

        return changed

    def print(self):
        """
        Ugly print the stats in this obj.
        """
        for mrt_e in self.longest_as_path:
            print(f"longest_as_path->prefix: {mrt_e.prefix}")
            print(f"longest_as_path->advt: {mrt_e.advt}")
            print(f"longest_as_path->as_path: {mrt_e.as_path}")
            print(f"longest_as_path->comm_set: {mrt_e.comm_set}")
            print(f"longest_as_path->next_hop: {mrt_e.next_hop}")
            print(f"longest_as_path->origin_asns: {mrt_e.origin_asns}")
            print(f"longest_as_path->peer_asn: {mrt_e.peer_asn}")
            print(f"longest_as_path->timestamp: {mrt_e.timestamp}")
            print(f"longest_as_path->updates: {mrt_e.updates}")
            print(f"longest_as_path->withdraws: {mrt_e.withdraws}")
        print("")

        for mrt_e in self.longest_comm_set:
            print(f"longest_comm_set->prefix: {mrt_e.prefix}")
            print(f"longest_comm_set->advt: {mrt_e.advt}")
            print(f"longest_comm_set->as_path: {mrt_e.as_path}")
            print(f"longest_comm_set->comm_set: {mrt_e.comm_set}")
            print(f"longest_comm_set->next_hop: {mrt_e.next_hop}")
            print(f"longest_comm_set->origin_asns: {mrt_e.origin_asns}")
            print(f"longest_comm_set->peer_asn: {mrt_e.peer_asn}")
            print(f"longest_comm_set->timestamp: {mrt_e.timestamp}")
            print(f"longest_comm_set->updates: {mrt_e.updates}")
            print(f"longest_comm_set->withdraws: {mrt_e.withdraws}")
        print("")

        for mrt_e in self.most_advt_prefixes:
            print(f"most_advt_prefixes->prefix: {mrt_e.prefix}")
            print(f"most_advt_prefixes->advt: {mrt_e.advt}")
            print(f"most_advt_prefixes->as_path: {mrt_e.as_path}")
            print(f"most_advt_prefixes->comm_set: {mrt_e.comm_set}")
            print(f"most_advt_prefixes->next_hop: {mrt_e.next_hop}")
            print(f"most_advt_prefixes->origin_asns: {mrt_e.origin_asns}")
            print(f"most_advt_prefixes->peer_asn: {mrt_e.peer_asn}")
            print(f"most_advt_prefixes->timestamp: {mrt_e.timestamp}")
            print(f"most_advt_prefixes->updates: {mrt_e.updates}")
            print(f"most_advt_prefixes->withdraws: {mrt_e.withdraws}")
        print("")

        for mrt_e in self.most_upd_prefixes:
            print(f"most_upd_prefixes->prefix: {mrt_e.prefix}")
            print(f"most_upd_prefixes->advt: {mrt_e.advt}")
            print(f"most_upd_prefixes->as_path: {mrt_e.as_path}")
            print(f"most_upd_prefixes->comm_set: {mrt_e.comm_set}")
            print(f"most_upd_prefixes->next_hop: {mrt_e.next_hop}")
            print(f"most_upd_prefixes->origin_asns: {mrt_e.origin_asns}")
            print(f"most_upd_prefixes->peer_asn: {mrt_e.peer_asn}")
            print(f"most_upd_prefixes->timestamp: {mrt_e.timestamp}")
            print(f"most_upd_prefixes->updates: {mrt_e.updates}")
            print(f"most_upd_prefixes->withdraws: {mrt_e.withdraws}")
        print("")

        for mrt_e in self.most_withd_prefixes:
            print(f"most_withd_prefixes->prefix: {mrt_e.prefix}")
            print(f"most_withd_prefixes->advt: {mrt_e.advt}")
            print(f"most_withd_prefixes->as_path: {mrt_e.as_path}")
            print(f"most_withd_prefixes->comm_set: {mrt_e.comm_set}")
            print(f"most_withd_prefixes->next_hop: {mrt_e.next_hop}")
            print(f"most_withd_prefixes->origin_asns: {mrt_e.origin_asns}")
            print(f"most_withd_prefixes->peer_asn: {mrt_e.peer_asn}")
            print(f"most_withd_prefixes->timestamp: {mrt_e.timestamp}")
            print(f"most_withd_prefixes->updates: {mrt_e.updates}")
            print(f"most_withd_prefixes->withdraws: {mrt_e.withdraws}")
        print("")

        for mrt_e in self.most_advt_origin_asn:
            print(f"most_advt_origin_asn->prefix: {mrt_e.prefix}")
            print(f"most_advt_origin_asn->advt: {mrt_e.advt}")
            print(f"most_advt_origin_asn->as_path: {mrt_e.as_path}")
            print(f"most_advt_origin_asn->comm_set: {mrt_e.comm_set}")
            print(f"most_advt_origin_asn->next_hop: {mrt_e.next_hop}")
            print(f"most_advt_origin_asn->origin_asns: {mrt_e.origin_asns}")
            print(f"most_advt_origin_asn->peer_asn: {mrt_e.peer_asn}")
            print(f"most_advt_origin_asn->timestamp: {mrt_e.timestamp}")
            print(f"most_advt_origin_asn->updates: {mrt_e.updates}")
            print(f"most_advt_origin_asn->withdraws: {mrt_e.withdraws}")
        print("")

        for mrt_e in self.most_advt_peer_asn:
            print(f"most_advt_peer_asn->prefix: {mrt_e.prefix}")
            print(f"most_advt_peer_asn->advt: {mrt_e.advt}")
            print(f"most_advt_peer_asn->as_path: {mrt_e.as_path}")
            print(f"most_advt_peer_asn->comm_set: {mrt_e.comm_set}")
            print(f"most_advt_peer_asn->next_hop: {mrt_e.next_hop}")
            print(f"most_advt_peer_asn->origin_asns: {mrt_e.origin_asns}")
            print(f"most_advt_peer_asn->peer_asn: {mrt_e.peer_asn}")
            print(f"most_advt_peer_asn->timestamp: {mrt_e.timestamp}")
            print(f"most_advt_peer_asn->updates: {mrt_e.updates}")
            print(f"most_advt_peer_asn->withdraws: {mrt_e.withdraws}")
        print("")

        for mrt_e in self.most_upd_peer_asn:
            print(f"most_upd_peer_asn->prefix: {mrt_e.prefix}")
            print(f"most_upd_peer_asn->advt: {mrt_e.advt}")
            print(f"most_upd_peer_asn->as_path: {mrt_e.as_path}")
            print(f"most_upd_peer_asn->comm_set: {mrt_e.comm_set}")
            print(f"most_upd_peer_asn->next_hop: {mrt_e.next_hop}")
            print(f"most_upd_peer_asn->origin_asns: {mrt_e.origin_asns}")
            print(f"most_upd_peer_asn->peer_asn: {mrt_e.peer_asn}")
            print(f"most_upd_peer_asn->timestamp: {mrt_e.timestamp}")
            print(f"most_upd_peer_asn->updates: {mrt_e.updates}")
            print(f"most_upd_peer_asn->withdraws: {mrt_e.withdraws}")
        print("")

        for mrt_e in self.most_withd_peer_asn:
            print(f"most_withd_peer_asn->prefix: {mrt_e.prefix}")
            print(f"most_withd_peer_asn->advt: {mrt_e.advt}")
            print(f"most_withd_peer_asn->as_path: {mrt_e.as_path}")
            print(f"most_withd_peer_asn->comm_set: {mrt_e.comm_set}")
            print(f"most_withd_peer_asn->next_hop: {mrt_e.next_hop}")
            print(f"most_withd_peer_asn->origin_asns: {mrt_e.origin_asns}")
            print(f"most_withd_peer_asn->peer_asn: {mrt_e.peer_asn}")
            print(f"most_withd_peer_asn->timestamp: {mrt_e.timestamp}")
            print(f"most_withd_peer_asn->updates: {mrt_e.updates}")
            print(f"most_withd_peer_asn->withdraws: {mrt_e.withdraws}")
        print("")

        for mrt_e in self.most_origin_asns:
            print(f"most_origin_asns->prefix: {mrt_e.prefix}")
            print(f"most_origin_asns->advt: {mrt_e.advt}")
            print(f"most_origin_asns->as_path: {mrt_e.as_path}")
            print(f"most_origin_asns->comm_set: {mrt_e.comm_set}")
            print(f"most_origin_asns->next_hop: {mrt_e.next_hop}")
            print(f"most_origin_asns->origin_asns: {mrt_e.origin_asns}")
            print(f"most_origin_asns->peer_asn: {mrt_e.peer_asn}")
            print(f"most_origin_asns->timestamp: {mrt_e.timestamp}")
            print(f"most_origin_asns->updates: {mrt_e.updates}")
            print(f"most_origin_asns->withdraws: {mrt_e.withdraws}")
        print("")

        print(f"file_list: {self.file_list}")

        print(f"timestamp: {self.timestamp}")

    def to_file(self, filename):
        """
        Serialise the MRT stats obj to JSON, save JSON as text file.
        """
        with open(filename, "w") as f:
            f.write(self.to_json())

    def to_json(self):
        """
        Serialise the MRT stats obj to JSON, and returns the JSON string.
        """
        json_data = {
            "longest_as_path": [
                mrt_e.to_json() for mrt_e in self.longest_as_path
            ],
            "longest_comm_set": [
                mrt_e.to_json() for mrt_e in self.longest_comm_set
            ],
            "most_advt_prefixes": [
                mrt_e.to_json() for mrt_e in self.most_advt_prefixes
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
            "file_list": self.file_list,
            "timestamp": self.timestamp,
        }
        return json.dumps(json_data)
