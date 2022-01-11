import json

class mrt_entry:

    def __init__(
        self,
        advertisements=0,
        as_path=[[]],
        community_set=[[]],
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
        self.community_set = community_set
        self.next_hop = next_hop
        self.origin_asns = origin_asns
        self.peer_asn = peer_asn
        self.prefix = prefix
        self.timestamp = timestamp
        self.updates = updates
        self.withdraws = withdraws

    @staticmethod
    def from_json(json_data):
        mrt_e = mrt_entry()
        mrt_e.advertisements = json_data["advertisements"]
        mrt_e.as_path = json_data["as_path"]
        mrt_e.community_set = json_data["community_set"]
        mrt_e.next_hop = json_data["next_hop"]
        mrt_e.prefix = json_data["prefix"]
        mrt_e.origin_asns = set(json_data["origin_asns"])
        mrt_e.peer_asn = json_data["peer_asn"]
        mrt_e.timestamp = json_data["timestamp"]
        mrt_e.updates = json_data["updates"]
        mrt_e.withdraws = json_data["withdraws"]
        return mrt_e

    def to_json(self):
        json_data = {
            "advertisements": self.advertisements,
            "as_path": self.as_path,
            "community_set": self.community_set,
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

        print(f"advertisements: {self.advertisements}")
        print(f"as_path: {self.as_path}")
        print(f"community_set: {self.community_set}")
        print(f"next_hop: {self.next_hop}")
        print(f"origin_asns: {self.origin_asns}")
        print(f"peer_asn: {self.peer_asn}")
        print(f"prefix: {self.prefix}")
        print(f"timestamp: {self.timestamp}")
        print(f"updates: {self.updates}")
        print(f"withdraws: {self.withdraws}")
        print("")
