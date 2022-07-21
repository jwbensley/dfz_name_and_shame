import ipaddress
from typing import List

from dnas.config import config as cfg

class bogon_ip:
    """
    Class to check if an IP subnet is a bogon address (meaning reserved by
    an RFC, the IETF, or IANA).
    """

    BOGON_V4_NETS: List[ipaddress.IPv4Network] = []
    for bogon in cfg.BOGONS_V4:
        bog_net = ipaddress.ip_network(bogon)
        if type(bog_net) != ipaddress.IPv4Network:
            raise TypeError(
                f"v4 bogon {bogon} is not a valid IPv4 subnet: {type(bog_net)}"
            )
        BOGON_V4_NETS.append(bog_net)

    BOGON_V6_NETS: List[ipaddress.IPv6Network] = []
    for bogon in cfg.BOGONS_V6:
        bog_net = ipaddress.ip_network(bogon)
        if type(bog_net) != ipaddress.IPv6Network:
            raise TypeError(
                f"v6 bogon {bogon} is not a valid IPv6 subnet: {type(bog_net)}"
            )
        BOGON_V6_NETS.append(bog_net)

    @staticmethod
    def is_v4_bogon(subnet: str = None) -> bool:
        """
        Return True if IP prefix is in a v4 bogon range, else False.
        Expects CIDR notation as string.
        """
        if not subnet:
            raise ValueError(
                f"Missing required options: subnet={subnet}"
            )

        if type(subnet) != str:
            raise TypeError(
                f"subnet is not a string: {type(subnet)}"
            )

        ip_net = ipaddress.ip_network(subnet)
        if type(ip_net) != ipaddress.IPv4Network:
            raise ValueError(
                f"{subnet} is not a valid IPv4 subnet: {type(ip_net)}"
            )

        for bogon_v4_net in bogon_ip.BOGON_V4_NETS:
            if ip_net.subnet_of(bogon_v4_net):
                return True
        return False

    @staticmethod
    def is_v6_bogon(subnet: str = None) -> bool:
        """
        Return True is IP prefix is in a v6 bogon range, else False.
        Expects CIDR notation as string.
        """
        if not subnet:
            raise ValueError(
                f"Missing required options: subnet={subnet}"
            )

        if type(subnet) != str:
            raise TypeError(
                f"subnet is not a string: {type(subnet)}"
            )

        ip_net = ipaddress.ip_network(subnet)
        if type(ip_net) != ipaddress.IPv6Network:
            raise ValueError(
                f"{subnet} is not a valid IPv6 subnet: {type(ip_net)}"
            )

        for bogon_v6_net in bogon_ip.BOGON_V6_NETS:
            if ip_net.subnet_of(bogon_v6_net):
                return True
        return False
