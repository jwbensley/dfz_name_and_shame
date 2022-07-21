import copy
import os
import re
import shutil
import sys
import unittest

sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)
from dnas.mrt_archives import mrt_archives
from dnas.mrt_entry import mrt_entry
from dnas.mrt_parser import mrt_parser

class test_mrt_entry(unittest.TestCase):

    def setUp(self):
        self.mrt_e = mrt_entry()

        """
        Copy the test files to the location they would be in,
        if we had downloaded them from the public archives:
        """
        self.upd_1_fn = "rcc23.updates.20220421.0200.gz"
        self.upd_1_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), self.upd_1_fn
        )
        if not os.path.isfile(self.upd_1_path):
            raise Exception(f"Test MRT file is not found: {self.upd_1_path}")

        self.entry_1_fn = "rcc23.updates.20220421.0200.mrt_entry.json"
        self.entry_1_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), self.entry_1_fn
        )
        if not os.path.isfile(self.entry_1_path):
            raise Exception(f"Test entry file is not found: {self.entry_1_path}")

        mrt_a = mrt_archives()
        for arch in mrt_a.archives:
            if arch.NAME == "RCC_23":
                os.makedirs(arch.MRT_DIR, exist_ok=True)
                self.upd_1_mrt = os.path.join(arch.MRT_DIR, self.upd_1_fn)

        shutil.copy2(self.upd_1_path, self.upd_1_mrt)
        self.mrt_s = mrt_parser.parse_upd_dump(self.upd_1_mrt)

    def test_init(self):
        self.assertIsInstance(self.mrt_e, mrt_entry)
        self.assertIsInstance(self.mrt_e.advt, int)
        self.assertIsInstance(self.mrt_e.as_path, list)
        self.assertEqual(len(self.mrt_e.as_path), 0)
        self.assertIsInstance(self.mrt_e.comm_set, list)
        self.assertEqual(len(self.mrt_e.comm_set), 0)
        self.assertEqual(self.mrt_e.next_hop, None)
        self.assertEqual(self.mrt_e.prefix, None)
        self.assertIsInstance(self.mrt_e.origin_asns, set)
        self.assertEqual(len(self.mrt_e.origin_asns), 0)
        self.assertEqual(self.mrt_e.peer_asn, None)
        self.assertEqual(self.mrt_e.timestamp, None)
        self.assertIsInstance(self.mrt_e.updates, int)
        self.assertIsInstance(self.mrt_e.withdraws, int)
        self.assertIsInstance(self.mrt_e.unknown_attrs, set)
        self.assertEqual(len(self.mrt_e.unknown_attrs), 0)

    def test_equal_to(self):
        e1 = copy.deepcopy(self.mrt_s.longest_as_path[0])
        e2 = copy.deepcopy(self.mrt_s.longest_as_path[0])

        self.assertIsInstance(e1, mrt_entry)
        self.assertIsInstance(e2, mrt_entry)
        self.assertRaises(ValueError, e1.equal_to)
        self.assertRaises(TypeError, e1.equal_to, 123)
        self.assertTrue(e1.equal_to(e2))

    def test_from_json(self):
        with open(self.entry_1_path) as f:
            json_data = f.read()
        self.assertIsInstance(json_data, str)

        e = mrt_entry()
        self.assertRaises(ValueError, e.from_json)
        self.assertRaises(TypeError, e.from_json, 123)
        e.from_json(json_data)

        self.assertTrue(e.equal_to(self.mrt_s.longest_as_path[0], True))

    def test_gen_timestamp(self):
        ret = mrt_entry.gen_timestamp()
        self.assertIsInstance(ret, str)
        self.assertTrue(
            re.match(
                (r"^(1999|20[0-2][0-9])(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])\."
                r"([0-1][0-9]|2[0-3])([0-5][0-9])$"),
                ret
            )
        )

    def test_to_json(self):
        with open(self.entry_1_path) as f:
            j1 = f.read()
        self.assertIsInstance(j1, str)

        j2 = self.mrt_s.longest_as_path[0].to_json()
        self.assertIsInstance(j2, str)
        self.maxDiff = None
        self.assertEqual(j1, j2)

    def tearDown(self):
        os.remove(self.upd_1_mrt)

if __name__ == '__main__':
    unittest.main()