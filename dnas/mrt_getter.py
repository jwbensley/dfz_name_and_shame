import datetime
import logging
import os
import requests

from dnas.config import config as cfg

class mrt_getter:
    """
    Class which can be used to get MRT files from public MRT archives.
    """

    @staticmethod
    def get_latest_rib(
        arch=None,
        replace=False,
    ):
        """
        Download the lastest RIB dump MRT from the given MRT archive.
        """
        if not arch:
            raise ValueError(
                f"Missing required options: arch={arch}"
            )

        filename = arch.gen_latest_rib_fn()
        url = arch.gen_rib_url(filename)
        outfile = os.path.normpath(arch.MRT_DIR + "/" + os.path.basename(url))
        mrt_getter.download_mrt(filename=outfile, replace=replace, url=url)
        return (outfile, url)

    @staticmethod
    def get_latest_upd(
        arch=None,
        replace=False,
    ):
        """
        Download the lastest update MRT file from the given MRT archive.
        """
        if not arch:
            raise ValueError(
                f"Missing required options: arch={arch}"
            )

        filename = self.gen_latest_upd_fn()
        url = arch.gen_upd_url(filename)
        outfile = os.path.normpath(arch.MRT_DIR + "/" + os.path.basename(url))
        mrt_getter.download_mrt(filename=outfile, replace=replace, url=url)
        return (outfile, url)

    @staticmethod
    def get_range_rib(
        arch=None,
        end_date=None,
        replace=False,
        start_date=None,
    ):
        """
        Download a range of RIB MRT dump files from an archive.
        All RIB MRT files from and inclusive of start_date to and inclusive
        of end_date are downloaded.

        start_date: In the MRT date format yyyymmdd.hhmm "20220129.0000"
        end_date: In the MRT date format yyyymmdd.hhmm "20220129.1230"
        """
        if (not arch or not start_date or not end_date):
            raise ValueError(
                f"Missing required options: arch={arch}, "
                f"start_date={start_date}, end_date={end_date}"
            )

        start = datetime.datetime.strptime(start_date, cfg.TIME_FORMAT)
        end = datetime.datetime.strptime(end_date, cfg.TIME_FORMAT)

        if end < start:
            raise ValueError(
                f"End date {end_date} is before start date {start_date}"
            )

        filenames = arch.gen_rib_url_range(end_date, start_date)
        downloaded = []
        logging.info(f"Downloaded 0/{len(filenames)}")

        for filename in filenames:
            url = arch.gen_rib_url(filename)
            outfile = os.path.normpath(arch.MRT_DIR + "/" + os.path.basename(url))

            if mrt_getter.download_mrt(
                filename=outfile, replace=replace, url=url
            ):
                downloaded.append((outfile, url))
                logging.info(f"Downloaded {len(downloaded)}/{len(filenames)}")

        return downloaded

    @staticmethod
    def get_range_upd(
        arch=None,
        end_date=None,
        replace=False,
        start_date=None,
    ):
        """
        Download a range of MRT update dump files from an MRT archive.
        All update MRT files from and inclusive of start_date to and inclusive
        of end_date will be downloaded.

        start_date: In the MRT date format yyyymmdd.hhmm "20220129.0000"
        end_date: In the MRT date format yyyymmdd.hhmm "20220129.1230"
        """

        if (not arch or not start_date or not end_date):
            raise ValueError(
                f"Missing required options: arch={arch}, "
                f"start_date={start_date}, end_date={end_date}"
            )

        start = datetime.datetime.strptime(start_date, cfg.TIME_FORMAT)
        end = datetime.datetime.strptime(end_date, cfg.TIME_FORMAT)

        if end < start:
            raise ValueError(
                f"End date {end_date} is before start date {start_date}"
            )

        filenames = arch.gen_rib_url_range(end_date, start_date)
        downloaded = []
        logging.info(f"Downloaded 0/{len(filenames)}")

        for filename in filenames:
            url = arch.gen_upd_url(filename)
            outfile = os.path.normpath(arch.MRT_DIR + "/" + os.path.basename(url))

            if mrt_getter.download_mrt(
                filename=outfile, replace=replace, url=url
            ):
                downloaded.append((outfile, url))
                logging.info(f"Downloaded {len(downloaded)}/{len(filenames)}")

        return downloaded

    @staticmethod
    def download_mrt(filename=None, replace=False, url=None):
        """
        Download an MRT file from the given url,
        and save it as the given filename.
        """
        if not url:
            raise ValueError("Missing URL")

        if not filename:
            raise ValueError("Missing output filename")

        os.makedirs(os.path.dirname(filename), exist_ok=True)

        if (not replace and os.path.exists(filename)):
            logging.info(f"Not overwriting existing file {filename}")
            return False

        """
        Need to open the HTTP connection before the local file, otherwise we
        create a blank local file, and then get a HTTP 404 for example, and if
        we use replace=false, then we're stuck with a bank local file, after
        the HTTP issue is fixed and the fail is available for download.
        """
        logging.info(f"Downloading {url} to {filename}")
        try:
            req = requests.get(url, stream=True)
        except requests.exceptions.ConnectionError as e:
            logging.info(f"Couldn't connect to MRT server: {e}")
            raise requests.exceptions.ConnectionError
        
        file_len = req.headers['Content-length']

        if req.status_code != 200:
            logging.info(f"HTTP error: {req.status_code}")
            logging.error(req.url)
            logging.error(req.text)
            logging.error(req.content)
            req.raise_for_status()

        if file_len is None or file_len == 0:
            logging.error(req.url)
            logging.error(req.text)
            logging.error(req.content)
            raise ValueError("Missing file length!")

        file_len = int(file_len)
        rcvd = 0
        logging.info(f"File size is {file_len/1024/1024:.7}MBs")
        progress = 0

        with open(filename, "wb") as f:
            for chunk in req.iter_content(chunk_size=1024):
                if req.status_code != 200:
                    logging.info(f"HTTP error: {req.status_code}")
                    logging.error(req.url)
                    logging.error(req.text)
                    logging.error(req.content)
                    f.close()
                    req.raise_for_status()

                rcvd += len(chunk)
                f.write(chunk)
                f.flush()

                if rcvd == file_len:
                    logging.debug(f"Downloaded {rcvd}/{file_len} ({(rcvd/file_len)*100}%)")
                elif ((rcvd/file_len)*100)//10 > progress:
                    logging.debug(f"Downloaded {rcvd}/{file_len} ({(rcvd/file_len)*100:.3}%)")
                    progress = ((rcvd/file_len)*100)//10

        return filename
