import datetime
import logging
import os
import requests

import sys
sys.path.append('./')
from config import config as cfg


class mrt_getter:
    """
    Class which can be used to get MRT files from public sources.
    """

    @staticmethod
    def get_ripe_latest_rib(
        base_url=None,
        dl_dir=None,
        file_ext=None,
        replace=False,
        rib_pfx=None,
    ):
        """
        Download the lastest RIB dump MRT from a RIPE MRT archive.
        RIB dumps are every 8 hours.
        """

        if (not base_url or
            not file_ext or
            not rib_pfx or
            not dl_dir):

            raise ValueError(
                f"Missing required options {base_url}, {rib_pfx}, {dl_dir}"
            )

        os.makedirs(dl_dir, exist_ok=True)
        if not os.path.isdir(dl_dir):
            raise ValueError(f"Output directory does not exist {dl_dir}")

        """
        When downloading rib dumps from RIPE, we calculate the time of the
        last whole 8 hour interval. RIPE RIB files are dumped every 8 hours,
        if it's 09.00 we're half way through the 08.00 to 16.00 time period,
        which will be available from 16.00 onwards. This means that the latest
        complete RIB dump we can download at 09.00 would be from the 00.00-08.00
        period and it would be called "0000". h_delta gets us back to 00.00.

        If this machine is in a different timezone to the archive server, an
        additional offset is required, RIPE_RIB_OFFSET.
        """
        hours = int(datetime.datetime.strftime(datetime.datetime.now(), "%H"))
        mod = hours % 8
        if mod == 0:
            h_delta = datetime.timedelta(hours=8)
        else:
            h_delta = datetime.timedelta(hours=(8 + mod))

        ym = datetime.datetime.strftime(datetime.datetime.now()-h_delta,"%Y.%m")
        ymd_hm = datetime.datetime.strftime(datetime.datetime.now()-h_delta,"%Y%m%d.%H00")

        rib_url = base_url + ym + rib_pfx + ymd_hm + file_ext
        filename = dl_dir + os.path.basename(rib_url)

        if (not replace and os.path.exists(filename)):
                logging.info(f"Skipping existing file {filename}")
        else:
            mrt_getter.download_mrt(filename=filename, url=rib_url)

        return (filename, rib_url)


    @staticmethod
    def get_ripe_latest_upd(
        base_url=None,
        dl_dir=None,
        file_ext=None,
        replace=False,
        upd_pfx=None,
    ):

        """
        Download the lastest update MRT file from a RIPE MRT archive.
        UPDATE dumps are every 5 minutes.
        """

        if (not base_url or
            not file_ext or
            not upd_pfx or
            not dl_dir):

            raise ValueError(
                f"Missing required options {base_url}, {upd_pfx}, {dl_dir}"
            )

        os.makedirs(dl_dir, exist_ok=True)
        if not os.path.isdir(dl_dir):
            raise ValueError(f"Output directory does not exist {dl_dir}")

        """
        When downloading updates from RIPE, we calculate the name of
        the last complete 5 minute update file. At 09.13 the last complete
        update file will be called "0905", it will be for the period
        09.05-09.10. So the filename can be calculatd as:
        "round down to the last 5 minute whole interval - another 5 minutes".

        If the current time is 09.15 (a round 15 minute interval) a dump
        should be available called "0910" for 09.10-09.15 period. The archive
        might be slow to update though.

        To be safe:
        At 09.13 this function will download the "0905" file (09.05-09.10).
        At 09.15 this function will download the "0905" file too.
        At 09.17 this function will download the "0910" file (09.10-09.15).

        If this machine is in a different timezone to the archive server, an
        additional offset is required, RCC_UPD_OFFSET.
        """
        minutes = int(datetime.datetime.strftime(datetime.datetime.now(), "%M"))
        mod = minutes % 5
        if mod == 0:
            m_delta = datetime.timedelta(minutes=10)
        else:
            m_delta = datetime.timedelta(minutes=(5 + mod))

        h_delta = datetime.timedelta(hours=cfg.RCC_UPD_OFFSET)

        ym = datetime.datetime.strftime(datetime.datetime.now()-h_delta,"%Y.%m")
        ymd_hm = datetime.datetime.strftime(datetime.datetime.now()-h_delta-m_delta,"%Y%m%d.%H%M")

        udp_url = base_url + ym + upd_pfx + ymd_hm + file_ext
        filename = dl_dir + os.path.basename(udp_url)

        if (not replace and os.path.exists(filename)):
                logging.info(f"Skipping existing file {filename}")
        else:
            mrt_getter.download_mrt(filename=filename, url=udp_url)

        return (filename, udp_url)

    @staticmethod
    def get_rv_latest_rib(
        base_url=None,
        dl_dir=None,
        file_ext=None,
        replace=False,
        rib_pfx=None,
    ):
        """
        Download the lastest RIB dump MRT from a route-views MRT archive.
        RIB dumps are every 2 hours.
        """

        if (not base_url or
            not file_ext or
            not rib_pfx or
            not dl_dir):

            raise ValueError(
                f"Missing required options {base_url}, {rib_pfx}, {dl_dir}"
            )

        os.makedirs(dl_dir, exist_ok=True)
        if not os.path.isdir(dl_dir):
            raise ValueError(f"Output directory does not exist {dl_dir}")

        """
        When downloading rib dumps from route-views, we calculate the
        time either 2 or 3 hours ago from now(). RV RIB files are dumped
        every 2 hours, if it's 09.00 we're half way through the 08.00 to 10.00
        time period, which will be available from 10.00 onwards. This means
        that the latest complete RIB dump we can download at 09.00 would be
        from 08.00. h_delta gets us back to 08.00.

        If this machine is in a different timezone to the archive server, an
        additional offset is required, RV_RIB_OFFSET.
        """
        hours = int(datetime.datetime.strftime(datetime.datetime.now(), "%H"))
        if hours % 2 != 0:
            hours = 3 + cfg.RV_RIB_OFFSET
        else:
            hours = 2 + cfg.RV_RIB_OFFSET
        h_delta = datetime.timedelta(hours=hours)

        ym = datetime.datetime.strftime(datetime.datetime.now()-h_delta,"%Y.%m")
        ymd_hm = datetime.datetime.strftime(datetime.datetime.now()-h_delta,"%Y%m%d.%H00")

        rib_url = base_url + ym + rib_pfx + ymd_hm + file_ext
        filename = dl_dir + os.path.basename(rib_url)

        if (not replace and os.path.exists(filename)):
                logging.info(f"Skipping existing file {filename}")
        else:
            mrt_getter.download_mrt(filename=filename, url=rib_url)

        return (filename, rib_url)

    @staticmethod
    def get_rv_latest_upd(
        base_url=None,
        dl_dir=None,
        file_ext=None,
        replace=False,
        upd_pfx=None,
    ):

        """
        Download the lastest update MRT file from a route-views MRT archive.
        UPDATE dumps are every 15 minutes.
        """

        if (not base_url or
            not file_ext or
            not upd_pfx or
            not dl_dir):

            raise ValueError(
                f"Missing required options {base_url}, {upd_pfx}, {dl_dir}"
            )

        os.makedirs(dl_dir, exist_ok=True)
        if not os.path.isdir(dl_dir):
            raise ValueError(f"Output directory does not exist {dl_dir}")

        """
        When downloading updates from route-views, we calculate the name of
        the last complete 15 minute update file. At 09.13 the last complete
        update file will be called "0845", it will be for the period
        08.45-09.00. So the filename can be calculatd as:
        "round down to the last 15 minute whole interval - another 15 minutes".

        If the current time is 09.15 (a round 15 minute interval) a dump
        should be available called "0900" for 09.00-09.15 period. The archive
        might be slow to update though.

        To be safe:
        At 09.13 this function will download the "0845" file (08.45-09.00).
        At 09.15 this function will download the "0845" file too.
        At 09.17 this function will download the "0900" file (09.00-09.15).

        If this machine is in a different timezone to the archive server, an
        additional offset is required, RV_UPD_OFFSET.
        """
        minutes = int(datetime.datetime.strftime(datetime.datetime.now(), "%M"))
        mod = minutes % 15
        if mod == 0:
            m_delta = datetime.timedelta(minutes=30)
        else:
            m_delta = datetime.timedelta(minutes=(15 + mod))

        h_delta = datetime.timedelta(hours=cfg.RV_UPD_OFFSET)

        ym = datetime.datetime.strftime(datetime.datetime.now()-h_delta,"%Y.%m")
        ymd_hm = datetime.datetime.strftime(datetime.datetime.now()-h_delta-m_delta,"%Y%m%d.%H%M")

        udp_url = base_url + ym + upd_pfx + ymd_hm + file_ext
        filename = dl_dir + os.path.basename(udp_url)

        if (not replace and os.path.exists(filename)):
                logging.info(f"Skipping existing file {filename}")
        else:
            mrt_getter.download_mrt(filename=filename, url=udp_url)

        return (filename, udp_url)

    @staticmethod
    def get_range_rv_upd(
        base_url=None,
        dl_dir=None,
        end_date=None,
        file_ext=None,
        replace=False,
        start_date=None,
        upd_pfx=None,
    ):

        """
        Download a range of MRT update dump files from route-views archive.
        All update MRT files from and inclusive of start_date to and inclusive
        of end_date.

        start_date: In the MRT date format yyyymmdd.hhmm "20220129.0915"
        end_date: In the MRT date format yyyymmdd.hhmm "20220129.0915"
        """

        if (not start_date or not end_date):
            raise ValueError(
                f"Missing required options {start_date}, {end_date}"
            )

        start = datetime.datetime.strptime(start_date, "%Y%m%d.%H%M")
        end = datetime.datetime.strptime(end_date, "%Y%m%d.%H%M")

        if end < start:
            raise ValueError(
                f"End date is before start date {start_date}, {end_date}"
            )

        diff = end - start
        count = diff.seconds // 900 # RV updates are every 15 minutes
        downloaded = []

        for i in range(0, count + 1):
            m_delta = datetime.timedelta(minutes=(i*15))
            ym = datetime.datetime.strftime(start+m_delta,"%Y.%m")
            ymd_hm = datetime.datetime.strftime(start+m_delta,"%Y%m%d.%H%M")

            udp_url = base_url + ym + upd_pfx + ymd_hm + file_ext
            filename = dl_dir + os.path.basename(udp_url)

            if (not replace and os.path.exists(filename)):
                    logging.info(f"Skipping existing file {filename}")
            else:
                mrt_getter.download_mrt(filename=filename, url=udp_url)
                downloaded.append((filename, udp_url))

        return downloaded


    @staticmethod
    def download_mrt(filename=None, url=None, debug=False):
        """
        Download an MRT file from the given url,
        and save it as the given filename.
        """

        if not url:
            raise ValueError("Missing ULR")

        if not filename:
            filename = os.path.basename(url)

        with open(filename, "wb") as f:
            logging.info(f"Downloading {url} to {filename}")
            try:
                req = requests.get(url, stream=True)
            except requests.exceptions.ConnectionError as e:
                if debug:
                    logging.debug(f"Couldn't connect to MRT server: {e}")
                f.close()
                raise requests.exceptions.ConnectionError

            file_len = req.headers['Content-length']

            if req.status_code != 200:
                if debug:
                    logging.debug(f"HTTP error: {req.status_code}")
                    logging.debug(req.url)
                    logging.debug(req.text)
                f.write(req.content)
                f.close()
                req.raise_for_status()

            if file_len is None:
                if debug:
                    logging.debug(req.url)
                    logging.debug(req.text)
                f.write(req.content)
                f.close()
                raise ValueError("Missing file length!")

            file_len = int(file_len)
            rcvd = 0
            logging.info(f"File size is {file_len/1024/1024:.7}MBs")
            progress = 0
            for chunk in req.iter_content(chunk_size=1024):

                if req.status_code != 200:
                    if debug:
                        logging.debug(f"HTTP error: {req.status_code}")
                        logging.debug(req.url)
                        logging.debug(req.text)
                    f.write(req.content)
                    f.close()
                    req.raise_for_status()

                rcvd += len(chunk)
                f.write(chunk)
                f.flush()

                if rcvd == file_len:
                    print(f"Downloaded {rcvd}/{file_len} ({(rcvd/file_len)*100}%)")
                elif ((rcvd/file_len)*100)//10 > progress:
                    print(f"\rDownloaded {rcvd}/{file_len} ({(rcvd/file_len)*100:.3}%)", end="\r")
                    progress = ((rcvd/file_len)*100)//10
