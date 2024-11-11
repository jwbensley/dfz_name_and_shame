import ipaddress
import os
import sys
import unittest

sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
)
from dnas.bogon_ip import bogon_ip
from dnas.config import config


class test_bogon_ip(unittest.TestCase):
    cfg = config()
    bi = bogon_ip()

    def test_init(self: "test_bogon_ip") -> None:
        self.assertIsInstance(self.bi, bogon_ip)

        self.assertIsInstance(self.bi.BOGON_V4_NETS, list)
        self.assertTrue(self.bi.BOGON_V4_NETS)
        for net_v4 in self.bi.BOGON_V4_NETS:
            self.assertIsInstance(net_v4, ipaddress.IPv4Network)

        self.assertIsInstance(self.bi.BOGON_V6_NETS, list)
        self.assertTrue(self.bi.BOGON_V6_NETS)
        for net_v6 in self.bi.BOGON_V6_NETS:
            self.assertIsInstance(net_v6, ipaddress.IPv6Network)

    def test_is_v4_bogon(self: "test_bogon_ip") -> None:
        self.assertRaises(ValueError, self.bi.is_v4_bogon, None)
        self.assertRaises(TypeError, self.bi.is_v4_bogon, 123)
        self.assertRaises(ValueError, self.bi.is_v4_bogon, "555.555.555.555")
        self.assertEqual(self.bi.is_v4_bogon("192.168.0.0/24"), True)
        self.assertEqual(self.bi.is_v4_bogon("11.22.33.0/24"), False)

    def test_is_v6_bogon(self: "test_bogon_ip") -> None:
        self.assertRaises(ValueError, self.bi.is_v6_bogon, None)
        self.assertRaises(TypeError, self.bi.is_v6_bogon, 123)
        self.assertRaises(
            ValueError, self.bi.is_v6_bogon, "HHHH:HHHH:HHHH:HHHH::/64"
        )
        self.assertEqual(self.bi.is_v6_bogon("2001:db8:ABCD::/48"), True)
        self.assertEqual(
            self.bi.is_v6_bogon("ABCD:ABCD:ABCD:ABCD::/64"), False
        )


if __name__ == "__main__":
    unittest.main()
