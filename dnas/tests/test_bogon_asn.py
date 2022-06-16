import os
import sys
import unittest

sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)
from dnas.bogon_asn import bogon_asn

class test_bogon_asn(unittest.TestCase):

    ba = bogon_asn()

    def test_init(self):
        self.assertIsInstance(self.ba, bogon_asn)

    def test_is_bogon(self):
        self.assertRaises(ValueError, self.ba.is_bogon, None)
        self.assertRaises(TypeError, self.ba.is_bogon, "abc")
        self.assertEqual(self.ba.is_bogon(65535), True)
        self.assertEqual(self.ba.is_bogon(1234567890), False)

if __name__ == '__main__':
    unittest.main()
