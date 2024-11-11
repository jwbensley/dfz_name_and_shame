import os
import sys
import unittest

sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
)
from dnas.unallocated_asn import unallocated_asn


class test_unallocated_asn(unittest.TestCase):
    ua = unallocated_asn()

    def test_init(self: "test_unallocated_asn") -> None:
        self.assertIsInstance(self.ua, unallocated_asn)
        self.assertTrue(len(self.ua.unallocated_ranges))

    def test_is_unallocated(self: "test_unallocated_asn") -> None:
        # 16 bit ASNs are all "allocated"
        self.assertFalse(self.ua.is_unallocated(12345))

        # 32 bit ASNs...

        # Before the start of a range
        self.assertFalse(self.ua.is_unallocated(402332))
        # Start of a range
        self.assertTrue(self.ua.is_unallocated(402333))
        # End of a range
        self.assertTrue(self.ua.is_unallocated(4199999999))
        # Beyond end of range
        self.assertFalse(self.ua.is_unallocated(4200000000))


if __name__ == "__main__":
    unittest.main()
