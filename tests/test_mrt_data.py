import unittest
import sys
sys.path.append('./')
from mrt_data import mrt_data
from mrt_entry import mrt_entry

class test_mrt_data(unittest.TestCase):

    mtr_d = mrt_data()

    def test_init(self):
        self.assertTrue(isinstance(self.mtr_d, mrt_data))
        self.assertTrue(isinstance(self.mtr_d.longest_as_path, list))
        self.assertTrue(isinstance(self.mtr_d.longest_as_path[0], mrt_entry))
        self.assertTrue(isinstance(self.mtr_d.longest_community_set, list))
        self.assertTrue(isinstance(self.mtr_d.longest_community_set[0], mrt_entry))
        self.assertTrue(isinstance(self.mtr_d.most_advt_prefixes, list))
        self.assertTrue(isinstance(self.mtr_d.most_advt_prefixes[0], mrt_entry))
        self.assertTrue(isinstance(self.mtr_d.most_upd_prefixes, list))
        self.assertTrue(isinstance(self.mtr_d.most_upd_prefixes[0], mrt_entry))
        self.assertTrue(isinstance(self.mtr_d.most_withd_prefixes, list))
        self.assertTrue(isinstance(self.mtr_d.most_withd_prefixes[0], mrt_entry))
        self.assertTrue(isinstance(self.mtr_d.most_advt_origin_asn, list))
        self.assertTrue(isinstance(self.mtr_d.most_advt_origin_asn[0], mrt_entry))
        self.assertTrue(isinstance(self.mtr_d.most_advt_peer_asn, list))
        self.assertTrue(isinstance(self.mtr_d.most_advt_peer_asn[0], mrt_entry))
        self.assertTrue(isinstance(self.mtr_d.most_upd_peer_asn, list))
        self.assertTrue(isinstance(self.mtr_d.most_upd_peer_asn[0], mrt_entry))
        self.assertTrue(isinstance(self.mtr_d.most_withd_peer_asn, list))
        self.assertTrue(isinstance(self.mtr_d.most_withd_peer_asn[0], mrt_entry))
        self.assertTrue(isinstance(self.mtr_d.most_origin_asns, list))
        self.assertTrue(isinstance(self.mtr_d.most_origin_asns[0], mrt_entry))

if __name__ == '__main__':
    unittest.main()
