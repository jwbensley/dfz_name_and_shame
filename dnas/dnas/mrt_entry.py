import datetime
import json
import typing

from dnas.config import config as cfg

class mrt_entry:
    """
    An MRT Entry object contains the prased BGP data which is a single data
    point for one of the stats in an MRT Stats object.
    """

    def __init__(
        self,
        advt=0,
        as_path=[],
        comm_set=[],
        filename=None,
        next_hop=None,
        prefix=None,
        origin_asns=set(),
        peer_asn=None,
        timestamp=None,
        updates=0,
        withdraws=0,
    ) -> None:

        self.advt = advt
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

    def equal_to(self, mrt_e: 'mrt_entry' = None, meta: bool = False) -> bool:
        """
        Return True if this MRT stat entry obj is the same as mrt_e, else False.
        Comparing meta data like filename and timestamp is option.
        """
        if not mrt_e:
            raise ValueError(
                f"Missing required arguments: mrt_e={mrt_e}"
            )

        if type(mrt_e) != mrt_entry:
            raise TypeError(
                f"mrt_e is not a stats entry: {type(mrt_e)}"
            )

        if self.advt != mrt_e.advt:
            print("a")
            return False

        if self.as_path != mrt_e.as_path:
            print(self.as_path)
            print(mrt_e.as_path)
            print("b")
            return False

        if self.comm_set != mrt_e.comm_set:
            print("c")
            return False

        if self.next_hop != mrt_e.next_hop:
            print("d")
            return False

        if self.origin_asns != mrt_e.origin_asns:
            print("e")
            return False

        if self.peer_asn != mrt_e.peer_asn:
            print("f")
            return False

        if self.prefix != mrt_e.prefix:
            print("g")
            return False

        if self.timestamp != mrt_e.timestamp:
            print("h")
            return False

        if self.updates != mrt_e.updates:
            print("i")
            return False

        if self.withdraws != mrt_e.withdraws:
            print("j")
            return False

        if meta:
            if self.filename != mrt_e.filename:
                print("k")
                return False

            if self.timestamp != mrt_e.timestamp:
                print("l")
                return False

        return True

    def from_json(self, json_str: str = None):
        """
        Parse a JSON str into this MRT stats entry obj.
        """
        if not json_str:
            raise ValueError(
                f"Missing required arguments: json_str={json_str}"
            )

        if type(json_str) != str:
            raise TypeError(
                f"json_str is not a string: {type(json_str)}"
            )

        json_data = json.loads(json_str)
        self.advt = json_data["advt"]
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

    @staticmethod
    def gen_timestamp() -> str:
        """
        Generate the timestamp to insert into a newly created MRT entry obj.
        """
        return datetime.datetime.now().strftime(cfg.TIME_FORMAT)

    def to_json(self) -> str:
        """
        Return this MRT entry obj serialised to a JSON str.
        """
        json_data = {
            "advt": self.advt,
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
        print(f"advt: {self.advt}")
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
