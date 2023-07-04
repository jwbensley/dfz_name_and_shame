from typing import Dict

class bogon_attr:
    """
    Class to check if a BGP attribute is "well known"/defined.
    """

    # https://www.iana.org/assignments/bgp-parameters/bgp-parameters.xhtml
    known_attrs = {
        1: "ORIGIN", # RFC4271
        2: "AS_PATH", #RFC4271
        3: "NEXT_HOP", # RFC4271
        4: "MULTI_EXIT_DISC", # RFC4271
        5: "LOCAL_PREF", # RFC4271
        6: "ATOMIC_AGGREGATE", # RFC4271
        7: "AGGREGATOR", # RFC4271
        8: "COMMUNITY", # RFC1997
        9: "ORIGINATOR_ID", # RFC4456
        10: "CLUSTER_LIST", # RFC4456
        #11: "DPA", # Deprecated
        #12: "ADVERTISER", # Deprecated
        #13: "RCID_PATH/CLUSTER_ID", # Deprecated
        14: "MP_REACH_NLRI", # RFC4760
        15: "MP_UNREACH_NLRI", # RFC4760
        16: "EXTENDED COMMUNITIES", # RFC4360
        17: "AS4_PATH", # RFC6793
        18: "AS4_AGGREGATOR", # RFC6793
        #19: "SAFI Specific Attribute", # Deprecated 
        #20: "Connector Attribute", # Deprecated
        #21: "AS_PATHLIMIT" # Deprecated
        22: "PMSI_TUNNEL", # RFC6514
        23: "Tunnel Encapsulation Attribute", # RFC5512
        24: "Traffic Engineering", # RFC5543
        25: "IPv6 Address Specific Extended Community", # RFC5701
        26: "AIGP", # RFC7311
        27: "PE Distinguisher Labels", # RFC6514
        #28: "BGP Entropy Label Capability Attribute", # Deprecated
        29: "BGP-LS Attribute", # RFC7752
        32: "LARGE_COMMUNITY", # RFC8092
        33: "BGPsec_Path", # RFC8205
        34:"BGP Community Container Attribute", # draft-ietf-idr-wide-bgp-communities
        35:"Only to Customer", # draft-ietf-idr-bgp-open-policy
        36:"BGP Domain Path", # draft-ietf-bess-evpn-ipvpn-interworking
        37: "SFP attribute", # RFC9015
        38: "BFD Discriminator", # RFC9026
        40: "BGP Prefix-SID", # RFC8669
        128: "ATTR_SET", # RFC6368
        255: "Reserved for development", #RFC2042
    }

    @staticmethod
    def is_unknown(attr: int = None) -> bool:
        """
        Return True is BGP attr ID is a unknown/bogon, else False
        """

        if type(attr) != int:
            raise TypeError(
                f"attr is not an int: {type(attr)}"
            )

        if attr in bogon_attr.known_attrs:
            return False
        else:
            return True
