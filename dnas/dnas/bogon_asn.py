class bogon_asn:
    """
    Class to check if an ASN is a bogon ASN (meaning reserved by an RFC,
    the IETF, or IANA).
    """

    @staticmethod
    def is_bogon(asn: int) -> bool:
        """
        Return True if ASN is a bogon ASN, else False.
        """
        if type(asn) != int:
            raise TypeError(f"{asn} is not an int: {type(asn)}")

        if asn == 0:  # RFC 7607
            return True
        elif asn == 23456:  # RFC 4893
            return True
        elif asn in range(64496, 64512):  # RFC 5398
            return True
        elif asn in range(65536, 65552):  # RFC 5398
            return True
        elif asn in range(64512, 65535):  # RFC 6996
            return True
        elif asn in range(4200000000, 4294967296):  # RFC 6996
            return True
        elif asn == 65535:  # RFC 6996
            return True
        elif asn == 4294967295:  # RFC 6996
            return True
        elif asn in range(65552, 131072):  # IANA reserved
            return True
        else:
            return False
