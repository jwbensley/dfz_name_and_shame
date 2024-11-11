import os
import sys
import tempfile
import unittest

import requests

sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
)
from dnas.mrt_getter import mrt_getter
from dnas.mrt_parser import mrt_parser


class test_mrt_getter(unittest.TestCase):
    valid_update_url = (
        "https://archive.routeviews.org/route-views.linx/bgpdata/2004.03/"
        "UPDATES/updates.20040318.1931.bz2"
    )
    invalid_path = (
        "http://archive.routeviews.org/mQFk79SrBI29HPUg0EgxFC17nkyZP4"
    )
    invalid_domain = "https://mQFk79SrBI29HPUg0EgxFC17nkyZP4.com"
    invalid_output_file = "/sbin/mQFk79SrBI29HPUg0EgxFC17nkyZP4"
    valid_output_file = tempfile.mktemp()
    mrt_entries = 791

    def test_download_file_invalid_args(self: "test_mrt_getter") -> None:
        """
        Required args missing
        """
        with self.assertRaises(TypeError):
            mrt_getter.download_file()  # type: ignore [call-arg]

    def test_download_file_no_perms(self: "test_mrt_getter") -> None:
        """
        Can't write to output path
        """
        with self.assertRaises(PermissionError):
            mrt_getter.download_file(
                filename=self.invalid_output_file,
                replace=False,
                url=self.valid_update_url,
            )

    def test_download_file_invalid_url(self: "test_mrt_getter") -> None:
        """
        Invalid URL
        """
        with self.assertRaises(requests.exceptions.HTTPError):
            mrt_getter.download_file(
                filename=self.valid_output_file,
                replace=False,
                url=self.invalid_path,
            )

    def test_download_file_invalid_domain(self: "test_mrt_getter") -> None:
        """
        Invalid domain
        """
        with self.assertRaises(requests.exceptions.ConnectionError):
            mrt_getter.download_file(
                filename=self.valid_output_file,
                replace=False,
                url=self.invalid_domain,
            )

    def test_download_file_valid(self: "test_mrt_getter") -> None:
        """
        Valid download details
        """
        mrt_getter.download_file(
            filename=self.valid_output_file,
            replace=True,
            url=self.valid_update_url,
        )
        self.assertTrue(os.path.isfile(self.valid_output_file))
        self.assertEqual(
            self.mrt_entries, mrt_parser.mrt_count(self.valid_output_file)
        )


if __name__ == "__main__":
    unittest.main()
