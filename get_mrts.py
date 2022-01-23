import datetime

import sys
sys.path.append('./')
from mrt_getter import mrt_getter
from redis_db import redis_db

def main():

	logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
	logging.info("Starting MRT downloader")
	g = mrt_getter()
	rdb = redis_db()

	#this_month = datetime.datetime.now().strftime("%Y-%m")
	#in_files = rdb.get_keys(f"IN_FILES")

	file_list = []







if __name__ == '__main__':
    main()