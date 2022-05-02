import ipaddress
import os
import sys
import unittest

sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)
from dnas.config import config
from dnas.bogon_ip import bogon_ip

class test_bogon_ip(unittest.TestCase):

    cfg = config()
    bi = bogon_ip()

    def test_init(self):
        self.assertIsInstance(self.bi, bogon_ip)

        self.assertIsInstance(self.bi.BOGON_V4_NETS, list)
        self.assertTrue(self.bi.BOGON_V4_NETS)
        for net in self.bi.BOGON_V4_NETS:
            self.assertIsInstance(net, ipaddress.IPv4Network)

        self.assertIsInstance(self.bi.BOGON_V6_NETS, list)
        self.assertTrue(self.bi.BOGON_V6_NETS)
        for net in self.bi.BOGON_V6_NETS:
            self.assertIsInstance(net, ipaddress.IPv6Network)

    def test_is_v4_bogon(self):
        self.assertRaises(ValueError, self.bi.is_v4_bogon, None)
        self.assertRaises(TypeError, self.bi.is_v4_bogon, 123)
        self.assertRaises(ValueError, self.bi.is_v4_bogon, "555.555.555.555")
        self.assertEqual(self.bi.is_v4_bogon("192.168.0.0/24"), True)
        self.assertEqual(self.bi.is_v4_bogon("11.22.33.0/24"), False)

    def test_is_v6_bogon(self):
        self.assertRaises(ValueError, self.bi.is_v6_bogon, None)
        self.assertRaises(TypeError, self.bi.is_v6_bogon, 123)
        self.assertRaises(ValueError, self.bi.is_v6_bogon, "HHHH:HHHH:HHHH:HHHH::/64")
        self.assertEqual(self.bi.is_v6_bogon("2001:db8:ABCD::/48"), True)
        self.assertEqual(self.bi.is_v6_bogon("ABCD:ABCD:ABCD:ABCD::/64"), False)

if __name__ == '__main__':
    unittest.main()
