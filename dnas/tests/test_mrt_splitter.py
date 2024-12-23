import gzip
import os
import sys
import unittest

sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
)

from dnas.mrt_parser import mrt_parser
from dnas.mrt_splitter import mrt_splitter


class test_mrt_splitter(unittest.TestCase):
    def setUp(self: "test_mrt_splitter") -> None:
        self.no_of_chunks = 8
        self.gz_filename = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "RRC23/",
            "rrc23.updates.20220501.2305.gz",
        )
        self.file_size = 30285

    def test_init(self: "test_mrt_splitter") -> None:
        self.assertRaises(ValueError, mrt_splitter, "")
        self.assertRaises(TypeError, mrt_splitter, 1.23)
        self.assertRaises(
            FileNotFoundError, mrt_splitter, "PewjWxSQavM7tCQbXIZlgcK9zXfr1H"
        )

        splitter = mrt_splitter(self.gz_filename)
        self.assertTrue(isinstance(splitter, mrt_splitter))
        self.assertTrue(isinstance(splitter.f, gzip.GzipFile))
        self.assertTrue(isinstance(splitter.filename, str))
        self.assertTrue(splitter.filename, True)
        self.assertEqual(splitter.filename, self.gz_filename)
        try:
            splitter.close()
        except StopIteration:
            pass

    def test_split(self: "test_mrt_splitter") -> None:
        splitter = mrt_splitter(self.gz_filename)

        self.assertRaises(ValueError, splitter.split, -1, -1)
        self.assertRaises(TypeError, splitter.split, -1)
        total, chunk_names = splitter.split(
            no_chunks=self.no_of_chunks,
            outdir=os.path.dirname(splitter.filename),
        )

        self.assertTrue(isinstance(total, int))
        self.assertEqual(total, self.file_size)
        self.assertTrue(isinstance(chunk_names, list))
        self.assertEqual(len(chunk_names), self.no_of_chunks)

        mrt_count = 0
        for filename in chunk_names:
            self.assertTrue(os.path.isfile(filename))
            mrt_count += mrt_parser.mrt_count(filename)
        self.assertEqual(total, mrt_count)
        try:
            splitter.close()
        except StopIteration:
            pass

        for filename in chunk_names:
            os.unlink(filename)


if __name__ == "__main__":
    unittest.main()
