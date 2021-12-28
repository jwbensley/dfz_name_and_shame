import gzip
import bz2

import os
import sys

from datetime import datetime

class MrtFormatError(Exception):
    '''
    Exception for invalid MRT formatted data.
    '''
    def __init__(self, msg=''):
        Exception.__init__(self)
        self.msg = msg

class Reader():
    '''
    Reader for MRT format data.
    '''

    def __init__(self, filename):

        # Magic Number
        GZIP_MAGIC = b'\x1f\x8b'
        BZ2_MAGIC = b'\x42\x5a\x68'

        self.data = None

        f = open(filename, 'rb')
        hdr = f.read(max(len(BZ2_MAGIC), len(GZIP_MAGIC)))
        f.close()

        if hdr.startswith(BZ2_MAGIC):
            self.f = bz2.BZ2File(filename, 'rb')
        elif hdr.startswith(GZIP_MAGIC):
            self.f = gzip.GzipFile(filename, 'rb')
        else:
            self.f = open(filename, 'rb')

    def close(self):
        '''
        Close file object and stop iteration.
        '''
        self.f.close()
        raise StopIteration

    def __iter__(self):
        return self

    def __next__(self):
        record = bytearray(self.f.read(12))

        '''
        Decoder for MRT header.
        '''
        if len(record) == 0:
            self.close()
        elif len(record) < 12:
            raise MrtFormatError(
                'Invalid MRT header length %d < 12 byte' % len(buf)
            )

        val = 0
        length = 0
        for i in record[-4:]:
            length = (val << 8) + i

        record += bytes(self.f.read(length))
        self.data = record

        return self


def main():

    if len(sys.argv) != 3:
        print(f"Wrong number or arguments supplied ({len(sys.argv)}), should be 3")
        print(f"{sys.argv[0]} /path/to/mrt/file <no_of_pieces_to_split_into>")
        exit(1)

    filename = sys.argv[1]
    no_of_chunks = int(sys.argv[2])

    chunk_files = []
    for i in range(0, no_of_chunks):
        chunk_name = filename + "_" + str(i)
        print(f"Opening {chunk_name}")
        f = open(chunk_name, "wb")
        chunk_files.append(f)

    entries = Reader(filename)
    for idx, entry in enumerate(entries):
        chunk_files[idx % no_of_chunks].write(entry.data)

    for i in range(0, no_of_chunks):
        chunk_files[i].close()


if __name__ == '__main__':
    main()