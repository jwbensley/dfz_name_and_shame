import datetime
import os
import sys
import unittest

sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)

from dnas.config import config as cfg
from dnas.mrt_archive import mrt_archive
from dnas.mrt_archives import mrt_archives


class test_mrt_archives (unittest.TestCase):


    def setUp(self):
        self.mrt_a = mrt_archives()
        self.cfg = cfg()

    def test_init(self):
        
        self.assertIsInstance(self.mrt_a, mrt_archives)
        self.assertIsInstance(self.mrt_a.archives, list)
        self.assertTrue(len(self.mrt_a.archives) > 0)
        for entry in self.mrt_a.archives:
            self.assertIsInstance(entry, mrt_archive)

    def test_arch_from_file_path(self):
        self.assertRaises(ValueError, self.mrt_a.arch_from_file_path)
        self.assertRaises(TypeError, self.mrt_a.arch_from_file_path, 123)

        arch = self.mrt_a.archives[0]
        filename = arch.gen_latest_upd_fn()
        filepath = os.path.normpath(arch.MRT_DIR + "/" + filename)
        
        ret = self.mrt_a.arch_from_file_path(filepath)
        self.assertIsInstance(ret, mrt_archive)
        self.assertEqual(ret, arch)

        ret = self.mrt_a.arch_from_file_path("/tmp/q39jojahaahfwolhfaa2")
        self.assertIsInstance(ret, bool)
        self.assertEqual(ret, False)

    def test_arch_from_url(self):
        self.assertRaises(ValueError, self.mrt_a.arch_from_url)
        self.assertRaises(TypeError, self.mrt_a.arch_from_url, 123)

        arch = self.mrt_a.archives[0]
        filename = arch.gen_latest_upd_fn()
        url = arch.gen_upd_url(filename)

        ret = self.mrt_a.arch_from_url(url)
        self.assertIsInstance(ret, mrt_archive)
        self.assertEqual(ret, arch)

        ret = self.mrt_a.arch_from_url("fw9f8whpwh3joj0jf3ojfw")
        self.assertIsInstance(ret, bool)
        self.assertEqual(ret, False)

    def test_get_arch_option(self):
        self.assertRaises(ValueError, self.mrt_a.get_arch_option)
        self.assertRaises(ValueError, self.mrt_a.get_arch_option, "abc")
        self.assertRaises(TypeError, self.mrt_a.get_arch_option, "abc", 123)

        self.assertRaises(
            ValueError, self.mrt_a.get_arch_option, "/tmp/9jw2f8wi", "abc"
        )

        arch = self.mrt_a.archives[0]
        filename = arch.gen_latest_upd_fn()
        filepath = os.path.normpath(arch.MRT_DIR + "/" + filename)

        ret = self.mrt_a.get_arch_option(filepath, "ENABLED")
        self.assertIsInstance(ret, bool)
        self.assertEqual(ret, True)

        self.assertRaises(
            AttributeError, self.mrt_a.get_arch_option, filepath, "hwiwewohh7"
        )

    def test_get_day_key(self):
        self.assertRaises(ValueError, self.mrt_a.get_day_key)
        self.assertRaises(TypeError, self.mrt_a.get_day_key, 123)

        self.assertRaises(ValueError, self.mrt_a.get_day_key, "/tmp/03oeiisks")

        arch = self.mrt_a.archives[0]
        filename = arch.gen_latest_upd_fn()
        filepath = os.path.normpath(arch.MRT_DIR + "/" + filename)

        ret = self.mrt_a.get_day_key(filepath)
        self.assertIsInstance(ret, str)
        ymd = datetime.datetime.strftime(
            datetime.datetime.now(), self.cfg.DAY_FORMAT
        )
        self.assertEqual(ret, arch.UPD_KEY + ":" + ymd)

    def test_is_rib_from_filename(self):
        self.assertRaises(ValueError, self.mrt_a.is_rib_from_filename)
        self.assertRaises(TypeError, self.mrt_a.is_rib_from_filename, 123)
        arch = self.mrt_a.archives[0]
        filename = arch.gen_latest_rib_fn()
        ret = self.mrt_a.is_rib_from_filename(filename)
        self.assertIsInstance(ret, bool)
        self.assertEqual(ret, True)
        filename = arch.gen_latest_upd_fn()
        ret = self.mrt_a.is_rib_from_filename(filename)
        self.assertIsInstance(ret, bool)
        self.assertEqual(ret, False)

if __name__ == '__main__':
    unittest.main()
