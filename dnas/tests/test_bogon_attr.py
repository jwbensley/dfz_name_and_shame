import os
import sys
import unittest

sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
)
from dnas.bogon_attr import bogon_attr


class test_bogon_attr(unittest.TestCase):
    ba = bogon_attr()

    def test_init(self: "test_bogon_attr") -> None:
        self.assertIsInstance(self.ba, bogon_attr)

    def test_is_unknown(self: "test_bogon_attr") -> None:
        self.assertRaises(TypeError, self.ba.is_unknown)
        self.assertRaises(TypeError, self.ba.is_unknown, "abc")
        for attr in self.ba.known_attrs:
            self.assertEqual(self.ba.is_unknown(attr), False)
        self.assertEqual(self.ba.is_unknown(11), True)


if __name__ == "__main__":
    unittest.main()
