import datetime
import logging
import os
from time import sleep
from typing import Literal, Tuple, Union

import requests
from dnas.config import config as cfg
from dnas.mrt_archive import mrt_archive


class mrt_getter:
    """
    Class which can be used to get MRT files from public MRT archives.
    """

    @staticmethod
    def get_latest_rib(
        arch: "mrt_archive",
        replace: bool = False,
    ) -> Tuple[str, str]:
        """
        Download the lastest RIB dump MRT from the given MRT archive.
        """
        if not arch:
            raise ValueError(f"Missing required options: arch={arch}")

        if type(arch) != mrt_archive:
            raise TypeError(f"arch is not an MRT archive: {type(arch)}")

        filename = arch.gen_latest_rib_fn()
        url = arch.gen_rib_url(filename)
        outfile = os.path.normpath(arch.MRT_DIR + "/" + os.path.basename(url))
        mrt_getter.download_file(filename=outfile, replace=replace, url=url)
        return (outfile, url)

    @staticmethod
    def get_latest_upd(
        arch: "mrt_archive",
        replace: bool = False,
    ) -> Tuple[str, str]:
        """
        Download the lastest update MRT file from the given MRT archive.
        """
        if not arch:
            raise ValueError(f"Missing required options: arch={arch}")

        if type(arch) != mrt_archive:
            raise TypeError(f"arch is not an MRT archive: {type(arch)}")

        filename = arch.gen_latest_upd_fn()
        url = arch.gen_upd_url(filename)
        outfile = os.path.normpath(arch.MRT_DIR + "/" + os.path.basename(url))
        mrt_getter.download_file(filename=outfile, replace=replace, url=url)
        return (outfile, url)

    @staticmethod
    def get_range_rib(
        arch: "mrt_archive",
        end_date: str,
        start_date: str,
        replace: bool = False,
    ) -> list[Tuple[str, str]]:
        """
        Download a range of RIB MRT dump files from an archive.
        All RIB MRT files from and inclusive of start_date to and inclusive
        of end_date are downloaded.

        start_date: In the MRT date format yyyymmdd.hhmm "20220129.0000"
        end_date: In the MRT date format yyyymmdd.hhmm "20220129.1230"
        """
        if not arch or not start_date or not end_date:
            raise ValueError(
                f"Missing required options: arch={arch}, "
                f"start_date={start_date}, end_date={end_date}"
            )

        if type(arch) != mrt_archive:
            raise TypeError(f"arch is not an MRT archive: {type(arch)}")

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
            outfile = os.path.normpath(
                arch.MRT_DIR + "/" + os.path.basename(url)
            )

            if mrt_getter.download_file(
                filename=outfile, replace=replace, url=url
            ):
                downloaded.append((outfile, url))
                logging.info(f"Downloaded {len(downloaded)}/{len(filenames)}")

        return downloaded

    @staticmethod
    def get_range_upd(
        arch: "mrt_archive",
        end_date: str,
        start_date: str,
        replace: bool = False,
    ) -> list[Tuple[str, str]]:
        """
        Download a range of MRT update dump files from an MRT archive.
        All update MRT files from and inclusive of start_date to and inclusive
        of end_date will be downloaded.

        start_date: In the MRT date format yyyymmdd.hhmm "20220129.0000"
        end_date: In the MRT date format yyyymmdd.hhmm "20220129.1230"
        """

        if not arch or not start_date or not end_date:
            raise ValueError(
                f"Missing required options: arch={arch}, "
                f"start_date={start_date}, end_date={end_date}"
            )

        if type(arch) != mrt_archive:
            raise TypeError(f"arch is not an MRT archive: {type(arch)}")

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
            outfile = os.path.normpath(
                arch.MRT_DIR + "/" + os.path.basename(url)
            )

            if mrt_getter.download_file(
                filename=outfile, replace=replace, url=url
            ):
                downloaded.append((outfile, url))
                logging.info(f"Downloaded {len(downloaded)}/{len(filenames)}")

        return downloaded

    @staticmethod
    def download_file(
        filename: str, url: str, replace: bool = False
    ) -> Union[str, Literal[False]]:
        """
        Download an MRT file from the given url,
        and save it as the given filename.
        """
        if not url:
            raise ValueError("Missing URL")

        if not filename:
            raise ValueError("Missing output filename")

        os.makedirs(os.path.dirname(filename), exist_ok=True)

        if not replace and os.path.exists(filename):
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
            """
            When pulling files from RIPE RIS, there are occasional timeouts.
            Retry a few times with a pause between attempts because this seems
            to almost always be a transient error.
            """
            retries = cfg.DL_RETIRES
            while retries > 0:
                try:
                    """
                    The default Accept-Encoding is gzip, which causes the server
                    to respond with a Content-Length which is not the full file
                    size. Replace this so we can later compare the file size:
                    """
                    req = requests.get(
                        url, headers={"Accept-Encoding": "*"}, stream=True
                    )
                    retries = 0
                except requests.exceptions.ReadTimeout as e:
                    retries -= 1
                    logging.info(
                        f"Request timeout connecting to HTTP server: {e}\n"
                        f"Remaining retires: {retries}\n"
                        f"Waiting {cfg.DL_DELAY} seconds..."
                    )
                    sleep(cfg.DL_DELAY)
        except requests.exceptions.ConnectionError as e:
            logging.info(f"Couldn't connect to HTTP server: {e}")
            raise requests.exceptions.ConnectionError

        if req.status_code != 200:
            logging.info(f"HTTP error: {req.status_code}")
            logging.error(req.url)
            logging.error(req.text)
            logging.error(req.content)
            req.raise_for_status()

        if os.path.exists(filename):
            local_size = os.path.getsize(filename)
        else:
            local_size = 0
        file_len = int(req.headers["Content-length"])

        if file_len is None or file_len == 0:
            logging.error(req.url)
            logging.error(req.text)
            logging.error(req.content)
            raise ValueError("Missing file length!")

        # Don't download if the file size has not changed
        if local_size == file_len:
            logging.warning(f"Not downloading, unchanged file size for {url}")
            return False

        rcvd = 0
        logging.info(f"File size is {file_len/1024/1024:.7}MBs")
        progress = 0.0

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
                    logging.debug(
                        f"Downloaded {rcvd}/{file_len} ({(rcvd/file_len)*100}%)"
                    )
                elif ((rcvd / file_len) * 100) // 10 > progress:
                    logging.debug(
                        f"Downloaded {rcvd}/{file_len} ({(rcvd/file_len)*100:.3}%)"
                    )
                    progress = ((rcvd / file_len) * 100) // 10

        return filename
