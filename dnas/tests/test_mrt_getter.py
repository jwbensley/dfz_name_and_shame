import os
import requests
import sys
import unittest

sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)
from dnas.mrt_getter import mrt_getter
from dnas.mrt_parser import mrt_parser


class test_mrt_splitter(unittest.TestCase):

    linx_update_url = "http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/UPDATES/updates.20220103.1345.bz2"
    no_records = 243007
    output_file = "/tmp/mQFk79SrBI29HPUg0EgxFC17nkyZP4"
    bad_output_file = "/sbin/mQFk79SrBI29HPUg0EgxFC17nkyZP4"
    invalid_path = "http://archive.routeviews.org/mQFk79SrBI29HPUg0EgxFC17nkyZP4"
    invalid_domain = "https://mQFk79SrBI29HPUg0EgxFC17nkyZP4.com"

    def test_download_mrt(self):

        with self.assertRaises(ValueError):
            mrt_getter.download_mrt()
        with self.assertRaises(PermissionError):
            mrt_getter.download_mrt(
                filename = self.bad_output_file,
                replace = False,
                url = self.linx_update_url
            )
        with self.assertRaises(requests.exceptions.HTTPError):
            mrt_getter.download_mrt(
                filename = self.output_file,
                replace = False,
                url = self.invalid_path
            )
        with self.assertRaises(requests.exceptions.ConnectionError):
            mrt_getter.download_mrt(
                filename = self.output_file,
                replace = False,
                url = self.invalid_domain
            )
        
        mrt_getter.download_mrt(
            filename = self.output_file,
            replace = False,
            url = self.linx_update_url
        )
        self.assertTrue(os.path.isfile(self.output_file))
        self.assertEqual(self.no_records, mrt_parser.mrt_count(self.output_file))

if __name__ == '__main__':
    unittest.main()
