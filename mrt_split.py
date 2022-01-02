import sys
from mrt_splitter import mrt_splitter

def main():

    if len(sys.argv) != 3:
        print(f"Wrong number or arguments supplied ({len(sys.argv)}), should be 3")
        print(f"{sys.argv[0]} /path/to/mrt/file <no_of_pieces_to_split_into>")
        exit(1)

    filename = sys.argv[1]
    no_of_chunks = int(sys.argv[2])

    splitter = mrt_splitter(filename)
    splitter.split(no_of_chunks)

if __name__ == '__main__':
    main()