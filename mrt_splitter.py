import bz2
import errno
import gzip
import os


class MrtFormatError(Exception):
    """
    Exception for invalid MRT formatted data.
    """

    def __init__(self, msg=''):
        Exception.__init__(self)
        self.msg = msg

class mrt_splitter():
    """
    Splitter for MRT files.
    Copy-pasta of the original mrtparer lib to split an MRT file into N files.
    """

    def __init__(self, filename=None, debug=False):

        # Magic Number
        GZIP_MAGIC = b'\x1f\x8b'
        BZ2_MAGIC = b'\x42\x5a\x68'

        self.debug = debug
        self.data = None
        self.f = None
        self.filename = filename

        if not isinstance(self.debug, bool):
            raise TypeError("Debug must be bool")

        if not self.filename:
            raise ValueError("MRT filename missing")

        if not os.path.isfile(filename):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), filename
            )

        f = open(filename, 'rb')
        hdr = f.read(max(len(BZ2_MAGIC), len(GZIP_MAGIC)))
        f.close()

        if hdr.startswith(BZ2_MAGIC):
            self.f = bz2.BZ2File(filename, 'rb')
            if self.debug:
                print("Is BZ2 file")
        elif hdr.startswith(GZIP_MAGIC):
            self.f = gzip.GzipFile(filename, 'rb')
            if self.debug:
                print("Is GZIP file")
        else:
            self.f = open(filename, 'rb')
            if self.debug:
                print("Is binary file")

    def close(self):
        """
        Close the open MRT file.
        """
        self.f.close()
        raise StopIteration

    def __iter__(self):
        return self

    def __next__(self):
        """
        Move to the next entri in the MRT file.
        """
        mrt_entry = bytearray(self.f.read(12))

        if len(mrt_entry) == 0:
            self.close()
        elif len(mrt_entry) < 12:
            raise MrtFormatError(
                'Invalid MRT header length %d < 12 byte' % len(buf)
            )

        val = 0
        length = 0
        for i in mrt_entry[-4:]:
            length = (length << 8) + i

        if self.debug:
            print(f"MRT entry length is {length}")
        mrt_entry += bytes(self.f.read(length))
        self.data = mrt_entry

        return self

    def split(self, no_of_chunks):
        """
        Split the MRT data into N equal chunks written to disk.
        Return the total number of MRT entries and a list of file names.
        """

        if not self.f:
            raise AttributeError("No MRT file is currently open")

        if (not no_of_chunks or
            not isinstance(no_of_chunks, int) or
            no_of_chunks < 1):
            raise ValueError("Number of chunks to split MRT file into must be a positive integer")

        basename = os.path.basename(self.filename)
        if basename[0:3].lower() == "rib":
            next(self) # Skip the peer table which is the first entry in the RIB dump

        chunk_names = []
        chunk_files = []
        for i in range(0, no_of_chunks):
            chunk_name = self.filename + "_" + str(i)
            chunk_names.append(chunk_name)
            if self.debug:
                print(f"Opening {chunk_name} for output")
            f = open(chunk_name, "wb")
            chunk_files.append(f)

        for idx, entry in enumerate(self):
            chunk_files[idx % no_of_chunks].write(entry.data)

        for i in range(0, no_of_chunks):
            chunk_files[i].close()

        total = idx + 1
        if self.debug:
            print(f"Split {total} mrt_entries into {no_of_chunks} files.")

        return total, chunk_names
