import json

class mrt_entry:

    def __init__(
        self,
        advertisements=0,
        as_path=[[]],
        comm_set=[[]],
        filename=None,
        next_hop=None,
        prefix=None,
        origin_asns=set(),
        peer_asn=None,
        timestamp=None,
        updates=0,
        withdraws=0,
    ):

        self.advertisements = advertisements
        self.as_path = as_path
        self.comm_set = comm_set
        self.filename = filename
        self.next_hop = next_hop
        self.origin_asns = origin_asns
        self.peer_asn = peer_asn
        self.prefix = prefix
        self.timestamp = timestamp
        self.updates = updates
        self.withdraws = withdraws

    def equal_to(self, mrt_e):
        """
        Return True if this MRT stat entry obj is the same as mrt_e, else False.
        Doesn't compare meta data like filename.
        """
        if self.advertisements != mrt_e.advertisements:
            return False

        if self.as_path != mrt_e.as_path:
            return False

        if self.comm_set != mrt_e.comm_set:
            return False

        if self.next_hop != mrt_e.next_hop:
            return False

        if self.origin_asns != mrt_e.origin_asns:
            return False

        if self.peer_asn != mrt_e.peer_asn:
            return False

        if self.prefix != mrt_e.prefix:
            return False

        if self.timestamp != mrt_e.timestamp:
            return False

        if self.updates != mrt_e.updates:
            return False

        if self.withdraws != mrt_e.withdraws:
            return False

        return True

    def from_json(self, json_str):
        """
        Parse a JSON str into this MRT stats entry obj.
        """
        json_data = json.loads(json_str)
        self.advertisements = json_data["advertisements"]
        self.as_path = json_data["as_path"]
        self.comm_set = json_data["comm_set"]
        self.filename = json_data["filename"] if ("filename" in json_data) else None ##### FIX ME
        self.next_hop = json_data["next_hop"]
        self.prefix = json_data["prefix"]
        self.origin_asns = set(json_data["origin_asns"])
        self.peer_asn = json_data["peer_asn"]
        self.timestamp = json_data["timestamp"]
        self.updates = json_data["updates"]
        self.withdraws = json_data["withdraws"]

    def to_json(self):
        """
        Return this MRT entry obj serialised to a JSON str.
        """
        json_data = {
            "advertisements": self.advertisements,
            "as_path": self.as_path,
            "comm_set": self.comm_set,
            "filename": self.filename,
            "next_hop": self.next_hop,
            "origin_asns": list(self.origin_asns),
            "peer_asn": self.peer_asn,
            "prefix": self.prefix,
            "timestamp": self.timestamp,
            "updates": self.updates,
            "withdraws": self.withdraws,
        }
        return json.dumps(json_data)

    def print(self):
        """
        Ugly print this MRT stats entry.
        """

        print(f"advertisements: {self.advertisements}")
        print(f"as_path: {self.as_path}")
        print(f"comm_set: {self.comm_set}")
        print(f"filename: {self.filename}")
        print(f"next_hop: {self.next_hop}")
        print(f"origin_asns: {self.origin_asns}")
        print(f"peer_asn: {self.peer_asn}")
        print(f"prefix: {self.prefix}")
        print(f"timestamp: {self.timestamp}")
        print(f"updates: {self.updates}")
        print(f"withdraws: {self.withdraws}")
        print("")
