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
from dnas.mrt_parser import mrt_parser
from dnas.mrt_stats import mrt_stats


class test_mrt_parser(unittest.TestCase):

    gz_filename = "tests/updates.20220409.0000.gz"
    gz_json = "tests/gz.json"
    json_filename = "tests/updates.20220409.0000.json"

    cfg = config()

    def test_init(self):
        mrt_p = mrt_parser()

    def test_parse_upd_dump(self):
        mrt_p = mrt_parser()
        parsed_stats = mrt_p.parse_upd_dump(self.gz_filename)
        parsed_stats.to_file(self.gz_json)
        test_stats = mrt_stats()
        test_stats.from_file(self.json_filename)
        self.assertTrue(parsed_stats.equal_to(test_stats))

if __name__ == '__main__':
    unittest.main()
