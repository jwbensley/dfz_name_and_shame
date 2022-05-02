import bz2
import gzip
import io
import os
import sys
import unittest

sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)

from dnas.mrt_splitter import mrt_splitter
from dnas.mrt_parser import mrt_parser


class test_mrt_splitter(unittest.TestCase):

    no_of_chunks = 8
    bz2_filename = "tests/ribv6.20211222.0600.bz2"
    gz_filename = "tests/rcc23.updates.20220501.2305.gz"
    bin_filename = "tests/ribv6.20211222.0600"
    file_size = 148014

    def test_init(self):

        with self.assertRaises(TypeError):
            mrt_splitter(self.bin_filename, 1.23)
        with self.assertRaises(ValueError):
            mrt_splitter("")
        with self.assertRaises(FileNotFoundError):
            mrt_splitter("PewjWxSQavM7tCQbXIZlgcK9zXfr1H")

        splitter = mrt_splitter(self.bz2_filename)
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


        splitter = mrt_splitter(self.gz_filename)
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

        splitter = mrt_splitter(self.bin_filename)
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

    def test_split(self):

        splitter = mrt_splitter(self.bin_filename)
        with self.assertRaises(ValueError):
            splitter.split(-1)
        total, chunk_names = splitter.split(self.no_of_chunks)
        
        self.assertTrue(isinstance(total, int))
        self.assertEqual(total, self.file_size)
        self.assertTrue(isinstance(chunk_names, list))
        self.assertEqual(len(chunk_names), self.no_of_chunks)

        mrt_count = 0
        for filename in chunk_names:
            self.assertTrue(os.path.isfile(filename))
            mrt_count += mrt_parser.mrt_count(filename)
        self.assertEqual(total, mrt_count)


if __name__ == '__main__':
    unittest.main()
