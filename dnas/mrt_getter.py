import datetime
import logging
import os
import requests

from dnas.config import config as cfg

class mrt_getter:
    """
    Class which can be used to get MRT files from public sources.
    """

    @staticmethod
    def get_ripe_latest_rib(
        arch=None,
        replace=False,
    ):
        """
        Download the lastest RIB dump MRT from a RIPE MRT archive.
        RIB dumps are every 8 hours.
        """

        if not arch:
            raise ValueError(
                f"Missing required options: arch={arch}"
            )

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
        mod = hours % (arch.RIB_INTERVAL // 60)
        if mod == 0:
            h_delta = datetime.timedelta(hours=(arch.RIB_INTERVAL // 60))
        else:
            h_delta = datetime.timedelta(hours=((arch.RIB_INTERVAL // 60) + mod))

        ym = datetime.datetime.strftime(datetime.datetime.now()-h_delta,"%Y.%m")
        ymd_hm = datetime.datetime.strftime(datetime.datetime.now()-h_delta,"%Y%m%d.%H00")

        url = arch.BASE_URL + ym + arch.RIB_URL + "/" + arch.RIB_PREFIX + ymd_hm + "." + arch.MRT_EXT
        filename = arch.MRT_DIR + os.path.basename(url)

        mrt_getter.download_mrt(filename=filename, replace=replace, url=url)
        return (filename, url)

    @staticmethod
    def get_ripe_latest_upd(
        arch=None,
        replace=False,
    ):
        """
        Download the lastest update MRT file from a RIPE MRT archive.
        UPDATE dumps are every 5 minutes.
        """

        if not arch:
            raise ValueError(
                f"Missing required options: arch={arch}"
            )

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
        mod = minutes % arch.UPD_INTERVAL
        if mod == 0:
            m_delta = datetime.timedelta(minutes=(arch.UPD_INTERVAL*2))
        else:
            m_delta = datetime.timedelta(minutes=(arch.UPD_INTERVAL + mod))

        h_delta = datetime.timedelta(hours=cfg.RCC_UPD_OFFSET)

        ym = datetime.datetime.strftime(datetime.datetime.now()-h_delta,"%Y.%m")
        ymd_hm = datetime.datetime.strftime(datetime.datetime.now()-h_delta-m_delta,"%Y%m%d.%H%M")

        url = arch.BASE_URL + ym + arch.UPD_URL + "/" + arch.UPD_PREFIX + ymd_hm + "." + arch.MRT_EXT
        filename = arch.MRT_DIR + os.path.basename(url)

        mrt_getter.download_mrt(filename=filename, replace=replace, url=url)
        return (filename, url)

    @staticmethod
    def get_ripe_range_rib(
        arch=None,
        end_date=None,
        replace=False,
        start_date=None,
    ):
        """
        Download a range of MRT RIB dump files from a RIPE archive.
        All update MRT files from and inclusive of start_date to and inclusive
        of end_date.

        start_date: In the MRT date format yyyymmdd.hhmm "20220129.0000"
        end_date: In the MRT date format yyyymmdd.hhmm "20220129.1230"
        """

        if (not arch or not start_date or not end_date):
            raise ValueError(
                f"Missing required options: arch={arch}, "
                f"start_date={start_date}, end_date={end_date}"
            )

        start = datetime.datetime.strptime(start_date, "%Y%m%d.%H%M")
        end = datetime.datetime.strptime(end_date, "%Y%m%d.%H%M")

        if end < start:
            raise ValueError(
                f"End date is before start date {start_date}, {end_date}"
            )

        diff = end - start
        count = int(diff.total_seconds()) // (arch.RIB_INTERVAL * 60)
        downloaded = []
        print(f"Done 0/{count}")

        for i in range(0, count + 1):
            m_delta = datetime.timedelta(minutes=(i*arch.RIB_INTERVAL))
            ym = datetime.datetime.strftime(start+m_delta,"%Y.%m")
            ymd_hm = datetime.datetime.strftime(start+m_delta,"%Y%m%d.%H%M")

            url = arch.BASE_URL + ym + arch.RIB_URL + "/" + arch.RIB_PREFIX + ymd_hm + "." + arch.MRT_EXT
            filename = arch.MRT_DIR + os.path.basename(url)

            if mrt_getter.download_mrt(
                filename=filename, replace=replace, url=url
            ):
                downloaded.append((filename, url))
                print(f"Done {i+1}/{count}")

        return downloaded

    @staticmethod
    def get_ripe_range_upd(
        arch=None,
        end_date=None,
        replace=False,
        start_date=None,
    ):
        """
        Download a range of MRT update dump files from a RIPE archive.
        All update MRT files from and inclusive of start_date to and inclusive
        of end_date.

        start_date: In the MRT date format yyyymmdd.hhmm "20220129.0000"
        end_date: In the MRT date format yyyymmdd.hhmm "20220129.1230"
        """

        if (not arch or not start_date or not end_date):
            raise ValueError(
                f"Missing required options: arch={arch}, "
                f"start_date={start_date}, end_date={end_date}"
            )

        start = datetime.datetime.strptime(start_date, "%Y%m%d.%H%M")
        end = datetime.datetime.strptime(end_date, "%Y%m%d.%H%M")

        if end < start:
            raise ValueError(
                f"End date is before start date {start_date}, {end_date}"
            )

        diff = end - start
        count = int(diff.total_seconds()) // (arch.UPD_INTERVAL * 60)
        downloaded = []
        print(f"Done 0/{count}")

        for i in range(0, count + 1):
            m_delta = datetime.timedelta(minutes=(i*arch.UPD_INTERVAL))
            ym = datetime.datetime.strftime(start+m_delta,"%Y.%m")
            ymd_hm = datetime.datetime.strftime(start+m_delta,"%Y%m%d.%H%M")

            url = arch.BASE_URL + ym + arch.UPD_URL + "/" + arch.UPD_PREFIX + ymd_hm + "." + arch.MRT_EXT
            filename = arch.MRT_DIR + os.path.basename(url)

            if mrt_getter.download_mrt(
                filename=filename, replace=replace, url=url
            ):
                downloaded.append((filename, url))
                print(f"Done {i+1}/{count}")

        return downloaded

    @staticmethod
    def get_rv_latest_rib(
        arch=None,
        replace=False,
    ):
        """
        Download the lastest RIB dump MRT from a route-views MRT archive.
        RIB dumps are every 2 hours.
        """

        if not arch:
            raise ValueError(
                f"Missing required options: arch={arch}"
            )

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
        if hours % (arch.RIB_INTERVAL // 60) != 0:
            hours = ((arch.RIB_INTERVAL // 60) + 1) + cfg.RV_RIB_OFFSET
        else:
            hours = (arch.RIB_INTERVAL // 60) + cfg.RV_RIB_OFFSET
        h_delta = datetime.timedelta(hours=hours)

        ym = datetime.datetime.strftime(datetime.datetime.now()-h_delta,"%Y.%m")
        ymd_hm = datetime.datetime.strftime(datetime.datetime.now()-h_delta,"%Y%m%d.%H00")

        url = arch.BASE_URL + ym + arch.RIB_URL + "/" + arch.RIB_PREFIX + ymd_hm + "." + arch.MRT_EXT
        filename = arch.MRT_DIR + os.path.basename(url)

        mrt_getter.download_mrt(filename=filename, replace=replace, url=url)
        return (filename, url)

    @staticmethod
    def get_rv_latest_upd(
        arch=None,
        replace=False,
    ):
        """
        Download the lastest update MRT file from a route-views MRT archive.
        UPDATE dumps are every 15 minutes.
        """

        if not arch:
            raise ValueError(
                f"Missing required options arch={arch}"
            )

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
        mod = minutes % arch.UPD_INTERVAL
        if mod == 0:
            m_delta = datetime.timedelta(minutes=2*arch.UPD_INTERVAL)
        else:
            m_delta = datetime.timedelta(minutes=(arch.UPD_INTERVAL + mod))

        h_delta = datetime.timedelta(hours=cfg.RV_UPD_OFFSET)

        ym = datetime.datetime.strftime(datetime.datetime.now()-h_delta,"%Y.%m")
        ymd_hm = datetime.datetime.strftime(datetime.datetime.now()-h_delta-m_delta,"%Y%m%d.%H%M")

        url = arch.BASE_URL + ym + arch.UPD_URL + "/" + arch.UPD_PREFIX + ymd_hm + "." + arch.MRT_EXT
        filename = arch.MRT_DIR + os.path.basename(url)

        mrt_getter.download_mrt(filename=filename, replace=replace, url=url)
        return (filename, url)

    @staticmethod
    def get_rv_range_rib(
        arch=None,
        end_date=None,
        replace=False,
        start_date=None,
    ):
        """
        Download a range of MRT RIB dump files from route-views archive.
        All update MRT files from and inclusive of start_date to and inclusive
        of end_date.

        start_date: In the MRT date format yyyymmdd.hhmm "20220129.0915"
        end_date: In the MRT date format yyyymmdd.hhmm "20220129.2359"
        """

        if (not arch or not start_date or not end_date):
            raise ValueError(
                f"Missing required options: arch={arch}, "
                f"start_date={start_date}, end_date={end_date}"
            )

        start = datetime.datetime.strptime(start_date, "%Y%m%d.%H%M")
        end = datetime.datetime.strptime(end_date, "%Y%m%d.%H%M")

        if end < start:
            raise ValueError(
                f"End date is before start date {start_date}, {end_date}"
            )

        diff = end - start
        count = int(diff.total_seconds()) // (arch.RIB_INTERVAL * 60)
        downloaded = []
        print(f"Done 0/{count}")

        for i in range(0, count + 1):
            m_delta = datetime.timedelta(minutes=(i*arch.RIB_INTERVAL))
            ym = datetime.datetime.strftime(start+m_delta,"%Y.%m")
            ymd_hm = datetime.datetime.strftime(start+m_delta,"%Y%m%d.%H%M")

            url = arch.BASE_URL + ym + arch.RIB_URL + "/" + arch.RIB_PREFIX + ymd_hm + "." + arch.MRT_EXT
            filename = arch.MRT_DIR + os.path.basename(url)

            if mrt_getter.download_mrt(
                filename=filename, replace=replace, url=url
            ):
                downloaded.append((filename, url))
                print(f"Done {i+1}/{count}")

        return downloaded

    @staticmethod
    def get_rv_range_upd(
        arch=None,
        end_date=None,
        replace=False,
        start_date=None,
    ):
        """
        Download a range of MRT update dump files from a route-views archive.
        All update MRT files from and inclusive of start_date to and inclusive
        of end_date.

        start_date: In the MRT date format yyyymmdd.hhmm "20220129.0915"
        end_date: In the MRT date format yyyymmdd.hhmm "20220129.2359"
        """

        if (not arch or not start_date or not end_date):
            raise ValueError(
                f"Missing required options: arch={arch}, "
                f"start_date={start_date}, end_date={end_date}"
            )

        start = datetime.datetime.strptime(start_date, "%Y%m%d.%H%M")
        end = datetime.datetime.strptime(end_date, "%Y%m%d.%H%M")

        if end < start:
            raise ValueError(
                f"End date is before start date {start_date}, {end_date}"
            )

        diff = end - start
        count = int(diff.total_seconds()) // (arch.UPD_INTERVAL * 60)
        downloaded = []
        print(f"Done 0/{count}")

        for i in range(0, count + 1):
            m_delta = datetime.timedelta(minutes=(i*arch.UPD_INTERVAL))
            ym = datetime.datetime.strftime(start+m_delta,"%Y.%m")
            ymd_hm = datetime.datetime.strftime(start+m_delta,"%Y%m%d.%H%M")

            url = arch.BASE_URL + ym + arch.UPD_URL + "/" + arch.UPD_PREFIX + ymd_hm + "." + arch.MRT_EXT
            filename = arch.MRT_DIR + os.path.basename(url)

            if mrt_getter.download_mrt(
                filename=filename, replace=replace, url=url
            ):
                downloaded.append((filename, url))
                print(f"Done {i+1}/{count}")

        return downloaded

    @staticmethod
    def download_mrt(filename=None, replace=False, url=None):
        """
        Download an MRT file from the given url,
        and save it as the given filename.
        """
        if not url:
            raise ValueError("Missing ULR")

        if not filename:
            filename = os.path.basename(url)

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
            logging.debug(req.url)
            logging.debug(req.text)
            logging.debug(req.content)
            req.raise_for_status()

        if file_len is None or file_len == 0:
            logging.debug(req.url)
            logging.debug(req.text)
            logging.debug(req.content)
            raise ValueError("Missing file length!")

        file_len = int(file_len)
        rcvd = 0
        logging.info(f"File size is {file_len/1024/1024:.7}MBs")
        progress = 0

        with open(filename, "wb") as f:
            for chunk in req.iter_content(chunk_size=1024):
                if req.status_code != 200:
                    logging.info(f"HTTP error: {req.status_code}")
                    logging.debug(req.url)
                    logging.debug(req.text)
                    logging.debug(req.content)
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

        return filename
