import os
import sys
import unittest

sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)
from dnas.mrt_stats import mrt_stats
from dnas.mrt_entry import mrt_entry
from dnas.mrt_parser import mrt_parser

class test_mrt_stats(unittest.TestCase):

    mtr_d = mrt_stats()
    test_rib_dump = "/tmp/bird-mrtdump_rib"
    test_rib_json = test_rib_dump + ".json"

    def test_init(self):
        self.assertIsInstance(self.mtr_d, mrt_stats)
        self.assertIsInstance(self.mtr_d.bogon_origin_asns, list)
        self.assertIsInstance(self.mtr_d.bogon_origin_asns[0], mrt_entry)
        self.assertIsInstance(self.mtr_d.bogon_prefixes, list)
        self.assertIsInstance(self.mtr_d.bogon_prefixes[0], mrt_entry)
        self.assertIsInstance(self.mtr_d.longest_as_path, list)
        self.assertIsInstance(self.mtr_d.longest_as_path[0], mrt_entry)
        self.assertIsInstance(self.mtr_d.longest_comm_set, list)
        self.assertIsInstance(self.mtr_d.longest_comm_set[0], mrt_entry)
        self.assertIsInstance(self.mtr_d.invalid_len, list)
        self.assertIsInstance(self.mtr_d.invalid_len[0], mrt_entry)
        self.assertIsInstance(self.mtr_d.most_advt_prefixes, list)
        self.assertIsInstance(self.mtr_d.most_advt_prefixes[0], mrt_entry)
        self.assertIsInstance(self.mtr_d.most_upd_prefixes, list)
        self.assertIsInstance(self.mtr_d.most_upd_prefixes[0], mrt_entry)
        self.assertIsInstance(self.mtr_d.most_withd_prefixes, list)
        self.assertIsInstance(self.mtr_d.most_withd_prefixes[0], mrt_entry)
        self.assertIsInstance(self.mtr_d.most_advt_origin_asn, list)
        self.assertIsInstance(self.mtr_d.most_advt_origin_asn[0], mrt_entry)
        self.assertIsInstance(self.mtr_d.most_advt_peer_asn, list)
        self.assertIsInstance(self.mtr_d.most_advt_peer_asn[0], mrt_entry)
        self.assertIsInstance(self.mtr_d.most_upd_peer_asn, list)
        self.assertIsInstance(self.mtr_d.most_upd_peer_asn[0], mrt_entry)
        self.assertIsInstance(self.mtr_d.most_withd_peer_asn, list)
        self.assertIsInstance(self.mtr_d.most_withd_peer_asn[0], mrt_entry)
        self.assertIsInstance(self.mtr_d.most_origin_asns, list)
        self.assertIsInstance(self.mtr_d.most_origin_asns[0], mrt_entry)

    def test_equal_to(self):
        upd_filename = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "rcc23.updates.20220501.2305.gz"
        )
        upd_json = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "rcc23.updates.20220501.2305.gz.json"
        )
        mrt_p = mrt_parser()
        parsed_stats = mrt_p.parse_upd_dump(self.upd_filename)
        test_stats = mrt_stats()
        test_stats.from_file(self.upd_json)
        self.assertTrue(parsed_stats.equal_to(test_stats))

    def test_to_json(self):
        mrt_s = mrt_parser.parse_upd_dump(self.test_rib_dump)
        json_str = mrt_s.to_json()
        self.assertIsInstance(json_str, str)

    def test_to_file(self):
        mrt_s = mrt_parser.parse_upd_dump(self.test_rib_dump)
        mrt_s.to_file(self.test_rib_json)
        self.assertTrue(os.path.isfile(self.test_rib_json))
        json_str = mrt_s.to_json()
        mrt_s.from_file(self.test_rib_json)
        self.assertEqual(json_str, mrt_s.to_json())

if __name__ == '__main__':
    unittest.main()
