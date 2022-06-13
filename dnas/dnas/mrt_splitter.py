import bz2
import errno
import gzip
import logging
import os
from typing import Any, List, NoReturn, Tuple

from dnas.mrt_archives import mrt_archives

class MrtFormatError(Exception):
    """
    Exception for invalid MRT formatted data.
    """

    def __init__(self, message: str = ""):
        Exception.__init__(self)
        self.message = message

    def __str__(self) -> str:
        if self.message:
            return self.message
        else:
            return "MrtFormatError"

class mrt_splitter:
    """
    Splitter for MRT files.
    Copy-pasta of the original mrtparer lib to split an MRT file into N files.
    """

    def __init__(self, filename: str = None) -> None:

        if not filename:
            raise ValueError("MRT filename missing")

        if type(filename) != str:
            raise TypeError(
                f"filename is not a string: {type(filename)}"
            )

        if not os.path.isfile(filename):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), filename
            )

        self.data: bytearray
        self.f: Any
        self.filename = filename

        # Magic Number
        GZIP_MAGIC = b'\x1f\x8b'
        BZ2_MAGIC = b'\x42\x5a\x68'

        f = open(filename, 'rb')
        hdr = f.read(max(len(BZ2_MAGIC), len(GZIP_MAGIC)))
        f.close()

        if hdr.startswith(BZ2_MAGIC):
            self.f = bz2.BZ2File(filename, 'rb')
            logging.debug("Assuming BZ2 file")
        elif hdr.startswith(GZIP_MAGIC):
            self.f = gzip.GzipFile(filename, 'rb')
            logging.debug("Assuming GZIP file")
        else:
            self.f = open(filename, 'rb')
            logging.debug("Assuming binary file")

    def close(self) -> NoReturn:
        """
        Close the open MRT file.
        """
        self.f.close()
        raise StopIteration

    def __iter__(self) -> 'mrt_splitter':
        return self

    def __next__(self) -> 'mrt_splitter':
        """
        Move to the next entry in the MRT file.
        """
        try:
            mrt_entry = bytearray(self.f.read(12))
        except EOFError:
            self.close()
            raise

        if len(mrt_entry) == 0:
            self.close()
        elif len(mrt_entry) < 12:
            raise MrtFormatError(
                f"Invalid MRT header length {len(mrt_entry)} < 12 bytes"
            )

        val = 0
        length = 0
        for i in mrt_entry[-4:]:
            length = (length << 8) + i

        mrt_entry += bytes(self.f.read(length))
        self.data = mrt_entry

        return self

    def split(self, no_chunks: int = None, outdir: str = None) -> Tuple[int, List[str]]:
        """
        Split the MRT data into N equal sized chunks written to disk.
        Return the total number of MRT entries and the list of chunk filenames.
        """
        if not self.f:
            raise AttributeError("No MRT file is currently open")

        if (not no_chunks or
            not isinstance(no_chunks, int) or
            no_chunks < 1):
            raise ValueError(
                f"Number of chunks to split MRT file into must be a positive "
                f"integer, not {no_chunks}"
            )

        # If no output dir is specified, write to the input directory:
        if not outdir:
            outdir = os.path.dirname(self.filename)

        # Skip the peer table which is the first entry in the RIB dump
        mrt_a = mrt_archives()
        if mrt_a.is_rib_from_filename(self.filename):
            next(self)

        chunk_filenames = []
        chunk_fds = []
        for i in range(0, no_chunks):
            chunk_name = self.filename + "_" + str(i)
            chunk_filenames.append(chunk_name)
            chunk_outpath = os.path.join(
                outdir,
                os.path.basename(self.filename) + "_" + str(i)
            )
            logging.debug(f"Opening {chunk_outpath} for output")
            f = open(chunk_outpath, "wb")
            chunk_fds.append(f)

        for idx, entry in enumerate(self):
            chunk_fds[idx % no_chunks].write(entry.data)

        for i in range(0, len(chunk_fds)):
            chunk_fds[i].close()

        total = idx + 1
        logging.debug(f"Split {total} mrt_entries into {no_chunks} files.")

        return total, chunk_filenames
