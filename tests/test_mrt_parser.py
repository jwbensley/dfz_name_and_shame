import unittest
import sys
sys.path.append('./')
from mrt_parser import mrt_parser

class test_mrt_data(unittest.TestCase):

    ##test_rib_dump = "/tmp/rib.20220103.1200.bz2"
    ##test_rib_chunk = "/tmp/rib.20220103.1200.bz2_0"
    test_rib_chunk = "/tmp/bird-mrtdump_rib"
    json_filename = test_rib_chunk + ".json"

    def test_to_json(self):
        rib_data = mrt_parser.parse_rib_dump(self.test_rib_chunk)
        json_data = mrt_parser.to_json(rib_data)
        self.assertTrue(isinstance(json_data, str))

    def test_from_file(self):
        rib_from_mrt = mrt_parser.parse_rib_dump(self.test_rib_chunk)
        json_from_mrt = mrt_parser.to_json(rib_from_mrt)
        mrt_parser.to_file(self.json_filename, rib_from_mrt)
        rib_from_file = mrt_parser.from_file(self.json_filename)
        json_from_file = mrt_parser.to_json(rib_from_file)
        self.assertTrue(isinstance(json_from_file, str))
        self.assertEqual(json_from_mrt, json_from_file)

if __name__ == '__main__':
    unittest.main()
