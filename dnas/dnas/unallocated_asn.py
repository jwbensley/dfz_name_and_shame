import logging

from dnas.config import config as cfg


class unallocated_asn:
    """
    Class to check if an ASN is unallocated by IANA to any RIR.
    """

    unallocated_ranges: list[range] = []

    def __init__(self: "unallocated_asn") -> None:
        """
        Load the unallocated ASN ranges
        """

        with open(cfg.unallocated_asns_file, "r") as unallocated_asns:
            for asn_tuple in unallocated_asns.readlines():
                min_asn, max_asn = map(int, asn_tuple.strip("()\n").split(","))
                self.unallocated_ranges.append(range(min_asn, max_asn + 1))
        logging.debug(
            f"Loaded {len(self.unallocated_ranges)} unallocated ASN tuples"
        )

    def is_unallocated(self: "unallocated_asn", asn: int) -> bool:
        """
        Return True if ASN is unallocated, else False.
        """
        if type(asn) != int:
            raise TypeError(f"{asn} is not an int: {type(asn)}")

        for unallocated_range in self.unallocated_ranges:
            if asn in unallocated_range:
                return True
        return False
