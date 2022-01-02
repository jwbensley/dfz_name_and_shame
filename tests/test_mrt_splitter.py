import unittest
import bz2
import gzip
import io
import sys
sys.path.append('./')
from mrt_splitter import mrt_splitter

class test_mrt_splitter(unittest.TestCase):

    
    no_of_chunks = 8
    

    def test_init(self):
        bz2_filename = "tests/ribv6.20211222.0600.bz2"
        splitter = mrt_splitter(bz2_filename, False)
        self.assertTrue(isinstance(splitter, mrt_splitter))
        self.assertTrue(isinstance(splitter.debug, bool))
        self.assertEqual(splitter.data, None)
        self.assertTrue(isinstance(splitter.f, bz2.BZ2File))
        self.assertTrue(isinstance(splitter.filename, str))
        self.assertTrue(splitter.filename, True)
        try:
            splitter.close()
        except StopIteration:
            pass

        gz_filename = "tests/ribv6.20211222.0600.gz"
        splitter = mrt_splitter(gz_filename, False)
        self.assertTrue(isinstance(splitter, mrt_splitter))
        self.assertTrue(isinstance(splitter.debug, bool))
        self.assertEqual(splitter.data, None)
        self.assertTrue(isinstance(splitter.f, gzip.GzipFile))
        self.assertTrue(isinstance(splitter.filename, str))
        self.assertTrue(splitter.filename, True)
        try:
            splitter.close()
        except StopIteration:
            pass

        bin_filename = "tests/ribv6.20211222.0600"
        splitter = mrt_splitter(bin_filename, False)
        self.assertTrue(isinstance(splitter, mrt_splitter))
        self.assertTrue(isinstance(splitter.debug, bool))
        self.assertEqual(splitter.data, None)
        self.assertTrue(isinstance(splitter.f, io.BufferedReader))
        self.assertTrue(isinstance(splitter.filename, str))
        self.assertTrue(splitter.filename, True)
        try:
            splitter.close()
        except StopIteration:
            pass

    #def test_split(self):
    #    total, chunks = self.splitter.split(self.no_of_chunks)
    #        self.assertEqual(self.splitter.f, io.TextIOWrapper)

if __name__ == '__main__':
    unittest.main()
