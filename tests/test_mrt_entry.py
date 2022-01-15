import unittest
import sys
sys.path.append('./')
from mrt_entry import mrt_entry

class test_mrt_entry(unittest.TestCase):

    mtr_e = mrt_entry()

    def test_init(self):
        self.assertTrue(isinstance(self.mtr_e, mrt_entry))
        self.assertTrue(isinstance(self.mtr_e.advertisements, int))
        self.assertTrue(isinstance(self.mtr_e.as_path, list))
        self.assertTrue(isinstance(self.mtr_e.as_path[0], list))
        self.assertTrue(isinstance(self.mtr_e.comm_set, list))
        self.assertTrue(isinstance(self.mtr_e.comm_set[0], list))
        self.assertEqual(self.mtr_e.next_hop, None)
        self.assertEqual(self.mtr_e.prefix, None)
        self.assertTrue(isinstance(self.mtr_e.origin_asns, set))
        self.assertEqual(self.mtr_e.peer_asn, None)
        self.assertEqual(self.mtr_e.timestamp, None)
        self.assertTrue(isinstance(self.mtr_e.updates, int))
        self.assertTrue(isinstance(self.mtr_e.withdraws, int))

if __name__ == '__main__':
    unittest.main()
