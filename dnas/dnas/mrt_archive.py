from functools import reduce
import datetime
import os
import re
from typing import List
import urllib.parse

from dnas.config import config as cfg

class mrt_archive:

    def __init__(
        self,
        BASE_URL: str = None,
        ENABLED: bool = False,
        MRT_DIR: str = None,
        MRT_EXT: str = None,
        NAME: str = None,
        RIB_GLOB: str = None,
        RIB_INTERVAL: int = None,
        RIB_KEY: str = None,
        RIB_OFFSET: int = None,
        RIB_PREFIX: str = None,
        RIB_URL: str = None,
        TYPE: str = None,
        UPD_GLOB: str = None,
        UPD_INTERVAL: int = None,
        UPD_KEY: str = None,
        UPD_OFFSET: int = None,
        UPD_PREFIX: str = None,
        UPD_URL: str = None,

    ):

        if type(BASE_URL) != str:
            raise TypeError(
                f"BASE_URL is not of type str: {type(BASE_URL)}"
            )
        if type(ENABLED) != bool:
            raise TypeError(
                f"ENABLED is not of type str: {type(ENABLED)}"
            )
        if type(MRT_DIR) != str:
            raise TypeError(
                f"MRT_DIR is not of type str: {type(MRT_DIR)}"
            )
        if type(MRT_EXT) != str:
            raise TypeError(
                f"MRT_EXT is not of type str: {type(MRT_EXT)}"
            )
        if type(NAME) != str:
            raise TypeError(
                f"NAME is not of type str: {type(NAME)}"
            )
        if type(RIB_GLOB) != str:
            raise TypeError(
                f"RIB_GLOB is not of type str: {type(RIB_GLOB)}"
            )
        if type(RIB_INTERVAL) != int:
            raise TypeError(
                f"RIB_INTERVAL is not of type str: {type(RIB_INTERVAL)}"
            )
        if type(RIB_KEY) != str:
            raise TypeError(
                f"RIB_KEY is not of type str: {type(RIB_KEY)}"
            )
        if type(RIB_OFFSET) != int:
            raise TypeError(
                f"RIB_OFFSET is not of type int: {type(RIB_OFFSET)}"
            )
        if type(RIB_PREFIX) != str:
            raise TypeError(
                f"RIB_PREFIX is not of type str: {type(RIB_PREFIX)}"
            )
        if type(RIB_URL) != str:
            raise TypeError(
                f"RIB_URL is not of type str: {type(RIB_URL)}"
            )
        if type(TYPE) != str:
            raise TypeError(
                f"TYPE is not of type str: {type(TYPE)}"
            )
        if type(UPD_GLOB) != str:
            raise TypeError(
                f"UPD_GLOB is not of type str: {type(UPD_GLOB)}"
            )
        if type(UPD_INTERVAL) != int:
            raise TypeError(
                f"UPD_INTERVAL is not of type str: {type(UPD_INTERVAL)}"
            )
        if type(UPD_KEY) != str:
            raise TypeError(
                f"UPD_KEY is not of type str: {type(UPD_KEY)}"
            )
        if type(UPD_OFFSET) != int:
            raise TypeError(
                f"UPD_OFFSET is not of type int: {type(UPD_OFFSET)}"
            )
        if type(UPD_PREFIX) != str:
            raise TypeError(
                f"UPD_PREFIX is not of type str: {type(UPD_PREFIX)}"
            )
        if type(UPD_URL) != str:
            raise TypeError(
                f"UPD_URL is not of type str: {type(UPD_URL)}"
            )

        self.BASE_URL = BASE_URL
        self.ENABLED = ENABLED
        self.MRT_DIR = MRT_DIR
        self.MRT_EXT = MRT_EXT
        self.NAME = NAME
        self.RIB_GLOB = RIB_GLOB
        self.RIB_INTERVAL = RIB_INTERVAL
        self.RIB_KEY = RIB_KEY
        self.RIB_OFFSET = RIB_OFFSET
        self.RIB_PREFIX = RIB_PREFIX
        self.RIB_URL = RIB_URL
        self.TYPE = TYPE
        self.UPD_GLOB = UPD_GLOB
        self.UPD_INTERVAL = UPD_INTERVAL
        self.UPD_KEY = UPD_KEY
        self.UPD_OFFSET = UPD_OFFSET
        self.UPD_PREFIX = UPD_PREFIX
        self.UPD_URL = UPD_URL

    @staticmethod
    def concat_url(url_chunks: List[str] = None) -> str:
        """
        Concatenate a list of strings into a single URL, and return as a
        single string.
        """
        if not url_chunks:
            raise ValueError(
                f"Missing required arguments: url_chunks={url_chunks}"
            )

        if type(url_chunks) != list:
            raise TypeError(
                f"List of URL chunks is not of type list: {type(url_chunks)}"
            )

        path = ""
        for chunk in url_chunks[1:]:
            path += chunk
        path = path.replace("///", "/")
        path = path.replace("//", "/")
        path = path.lstrip("/")

        if url_chunks[0][-1] != "/":
            return url_chunks[0] + "/" + path
        else:
            return url_chunks[0] + path

    def gen_latest_rib_fn(self) -> str:
        """
        Generate and return the filename for the newest/most recent RIB dump
        from this object's archive.
        """
        if self.TYPE == "RIPE":
            return self.gen_latest_rib_fn_ripe()
        elif self.TYPE == "RV":
            return self.gen_latest_rib_fn_rv()
        elif self.TYPE == "AS57355":
            return self.gen_latest_rib_fn_as57355()
        else:
            raise ValueError(f"Unknown MRT archive type {self.TYPE}")

    def gen_latest_rib_fn_as57355(self) -> str:
        """
        Generate and return the filename for the newest/most recent RIB dump
        from an AS57355 MRT archive.

        RIPE RIB dumps are every 1 hours.

        When downloading rib dumps from AS57355, calculate the time of the last
        whole 1 hour interval. AS57355 RIB files are dumped every 1 hours.
        If it's 09.30 we're half way through the 09.00 to 10.00 time period,
        which will be available from 10.00 onwards.
        This means that the latest complete RIB dump we can download at 09.00
        would be from the 08.00-09.00 period and it would be called "0800".
        h_delta gets us back to 08.00.

        If this machine is in a different timezone to the archive server, an
        additional offset is required, RIB_OFFSET.
        """
        hours = int(datetime.datetime.strftime(datetime.datetime.now(), "%H"))
        mod = hours % (self.RIB_INTERVAL // 60)
        if mod == 0:
            h_delta = datetime.timedelta(
                hours = (self.RIB_INTERVAL // 60) + (self.RIB_OFFSET // 60)
            )
        else:
            h_delta = datetime.timedelta(
                hours = (self.RIB_INTERVAL // 60) + mod + (self.RIB_OFFSET // 60)
            )

        ymd_hm = datetime.datetime.strftime(
            datetime.datetime.now() - h_delta,
            "%Y%m%d.%H00"
        )

        return self.RIB_PREFIX + ymd_hm + "." + self.MRT_EXT

    def gen_latest_rib_fn_ripe(self) -> str:
        """
        Generate and return the filename for the newest/most recent RIB dump
        from a RIPE MRT archive.

        RIPE RIB dumps are every 8 hours.

        When downloading rib dumps from RIPE, calculate the time of the last
        whole RIB dump interval. RIPE RIB files are dumped every 8 hours, on
        round 8 hour intervals.
        If it's 09.00 we're part way through the 08.00 to 16.00 time period,
        which will be available from 16.00 onwards.
        This means that the latest complete RIB dump we can download at 09.00
        would be from the 00.00-08.00 period and it would be called "0000".
        h_delta gets us back to 00.00.

        If this machine is in a different timezone to the archive server, an
        additional offset is required, RIB_OFFSET.
        """
        hours = int(datetime.datetime.strftime(datetime.datetime.now(), "%H"))
        mod = hours % (self.RIB_INTERVAL // 60)
        if mod == 0:
            h_delta = datetime.timedelta(
                hours = (self.RIB_INTERVAL // 60) + (self.RIB_OFFSET // 60)
            )
        else:
            h_delta = datetime.timedelta(
                hours = (self.RIB_INTERVAL // 60) + mod + (self.RIB_OFFSET // 60)
            )

        ymd_hm = datetime.datetime.strftime(
            datetime.datetime.now() - h_delta,
            "%Y%m%d.%H00"
        )

        return self.RIB_PREFIX + ymd_hm + "." + self.MRT_EXT

    def gen_latest_rib_fn_rv(self) -> str:
        """
        Generate and return the filename for the newest/most recent RIB dump
        from a route-views MRT archive.

        Route-views RIB dumps are every 2 hours.

        When downloading rib dumps from route-views, we calculate the
        time either 2 or 3 hours ago from now(). RV RIB files are dumped
        every 2 hours on the even hour interval. This means that if it's
        09.00 we're half way through the 08.00 to 10.00 time period, which
        will be available from 10.00 onwards.
        This means that the latest complete RIB dump we can download at 09.00
        would be from 06.00 to 08.00. h_delta gets us back to 06.00.

        If this machine is in a different timezone to the archive server, an
        additional offset is required, RIB_OFFSET.
        """
        hours = int(datetime.datetime.strftime(datetime.datetime.now(), "%H"))
        mod = hours % (self.RIB_INTERVAL // 60)
        if mod == 0:
            h_delta = datetime.timedelta(
                hours = (self.RIB_INTERVAL // 60) + (self.RIB_OFFSET // 60)
            )
        else:
            h_delta = datetime.timedelta(
                hours = (self.RIB_INTERVAL // 60) + mod + (self.RIB_OFFSET // 60)
            )

        ymd_hm = datetime.datetime.strftime(
            datetime.datetime.now() - h_delta,
            "%Y%m%d.%H00"
        )

        return self.RIB_PREFIX + ymd_hm + "." + self.MRT_EXT

    def gen_latest_upd_fn(self) -> str:
        """
        Generate and return the filename for the newest/most recent UPDATE dump
        from a this object's archive.
        """
        if self.TYPE == "RIPE":
            return self.gen_latest_upd_fn_ripe()
        elif self.TYPE == "RV":
            return self.gen_latest_upd_fn_rv()
        elif self.TYPE == "AS57355":
            return self.gen_latest_upd_fn_as57355()
        else:
            raise ValueError(f"Unknown MRT archive type {self.TYPE}")

    def gen_latest_upd_fn_as57355(self) -> str:
        """
        Generate and return the filename of the newest/most recent UPDATE dump
        for an AS57355 MRT archive.

        AS57355 UPDATE dumps are every 10 minutes.

        When downloading updates from AS57355, we calculate the name of
        the last complete update file. At 09.13 the last complete
        update file will be called "0900", it will be for the period
        09.00-09.10. So the filename can be calculatd as:
        "round down to the last 10 minute whole interval - another 10 minutes".

        If the current time is 09.20 (a round 10 minute interval) a dump
        should be available called "0910" for 09.10-09.20 period. The archive
        might be slow to update though.

        To be safe:
        At 09.13 this function will download the "0850" file (08.50-09.00).
        At 09.20 this function will download the "0850" file too.
        At 09.23 this function will download the "0900" file (09.00-09.10).

        If this machine is in a different timezone to the archive server, an
        additional offset is required, UPD_OFFSET.
        """
        minutes = int(datetime.datetime.strftime(datetime.datetime.now(), "%M"))
        mod = minutes % self.UPD_INTERVAL
        if mod == 0:
            m_delta = datetime.timedelta(minutes=(self.UPD_INTERVAL * 2))
        else:
            m_delta = datetime.timedelta(minutes=((self.UPD_INTERVAL * 2) + mod))

        h_delta = datetime.timedelta(minutes=self.UPD_OFFSET)

        ymd_hm = datetime.datetime.strftime(
            datetime.datetime.now() - h_delta - m_delta,
            cfg.TIME_FORMAT
        )

        return self.UPD_PREFIX + ymd_hm + "." + self.MRT_EXT

    def gen_latest_upd_fn_ripe(self) -> str:
        """
        Generate and return the filename of the newest/most recent UPDATE dump
        for a RIPE MRT archive.

        RIPE UPDATE dumps are every 5 minutes.

        When downloading updates from RIPE, we calculate the name of
        the last complete 5 minute update file. At 09.13 the last complete
        update file will be called "0905", it will be for the period
        09.05-09.10. So the filename could be calculatd as:
        "round down to the last 5 minute whole interval - another 5 minutes".

        If the current time is 09.15 (a round 15 minute interval) a dump
        should be available called "0910" for 09.10-09.15 period. The archive
        might be slow to update though. To be safe, always generate the time
        2x the interval period.

        Example:
        At 09.13 this function will download the "0900" file (09.00-09.05).
        At 09.15 this function will download the "0905" file (09.05-09.10)
        At 09.17 this function will download the "0905" file too.

        If this machine is in a different timezone to the archive server, an
        additional offset is required, UPD_OFFSET.
        """
        minutes = int(datetime.datetime.strftime(datetime.datetime.now(), "%M"))
        mod = minutes % self.UPD_INTERVAL
        if mod == 0:
            m_delta = datetime.timedelta(minutes=(self.UPD_INTERVAL * 2))
        else:
            m_delta = datetime.timedelta(minutes=((self.UPD_INTERVAL * 2) + mod))

        h_delta = datetime.timedelta(minutes=self.UPD_OFFSET)
        ymd_hm = datetime.datetime.strftime(
            datetime.datetime.now() - h_delta - m_delta,
            cfg.TIME_FORMAT
        )
        return self.UPD_PREFIX + ymd_hm + "." + self.MRT_EXT

    def gen_latest_upd_fn_rv(self) -> str:
        """
        Generate and return the filename of the newest/most recent UPDATE dump
        for a route-views MRT archive.

        Route-views UPDATE dumps are every 15 minutes.

        When downloading updates from route-views, we calculate the name of
        the last complete 15 minute update file. At 09.13 the last complete
        update file will be called "0845", it will be for the period
        08.45-09.00. So the filename could be calculatd as:
        "round down to the last 15 minute whole interval - another 15 minutes".

        If the current time is 09.15 (a round 15 minute interval) a dump
        should be available called "0900" for 09.00-09.15 period. The archive
        might be slow to update though. To be safe, generate the time two
        intervals ago.

        To be safe:
        At 09.13 this function will download the "0830" file (08.30-08.45).
        At 09.15 this function will download the "0845" file (08.45-09.00)
        At 09.17 this function will download the "0845" file too.

        If this machine is in a different timezone to the archive server, an
        additional offset is required, RV_UPD_OFFSET.
        """
        minutes = int(datetime.datetime.strftime(datetime.datetime.now(), "%M"))
        mod = minutes % self.UPD_INTERVAL
        if mod == 0:
            m_delta = datetime.timedelta(minutes=(self.UPD_INTERVAL * 2))
        else:
            m_delta = datetime.timedelta(minutes=((self.UPD_INTERVAL * 2) + mod))

        h_delta = datetime.timedelta(minutes=self.UPD_OFFSET)

        ymd_hm = datetime.datetime.strftime(
            datetime.datetime.now() - h_delta - m_delta, cfg.TIME_FORMAT
        )
        return self.UPD_PREFIX + ymd_hm + "." + self.MRT_EXT

    def gen_rib_fn_date(self, ymd_hm: str = None) -> str:
        """
        Generate the filename of a RIB MRT file, for the given date and time.
        This function is MRT archive type agnostic.
        """
        if not ymd_hm:
            raise ValueError(
                f"Missing required arguments: ymd_hm={ymd_hm}"
            )
        mrt_archive.valid_ymd_hm(ymd_hm)
        return f"{self.RIB_PREFIX}{ymd_hm}.{self.MRT_EXT}"

    def gen_rib_fns_day(self, ymd: str = None) -> List[str]:
        """
        Generate a list of all the RIB MRT filenames for a this MRT archive,
        for a specific day. This function is MRT archive type agnostic.
        """
        if not ymd:
            raise ValueError(
                f"Missing required arguments: ymd={ymd}"
            )
        mrt_archive.valid_ymd(ymd)

        filenames = []
        minutes = 0
        while(minutes < 1440):
            datetime.timedelta(minutes=minutes)
            hh = f"{minutes//60:02}"
            mm = f"{minutes%60:02}"
            ymd_hm = f"{ymd}.{hh}{mm}"
            filenames.append(self.gen_rib_fn_date(ymd_hm))
            minutes += self.RIB_INTERVAL
        return filenames

    def gen_rib_fns_range(self, end_date: str = None, start_date: str = None) -> List[str]:
        """
        Generate and return a list of filenames for a range of RIB MRT dumps,
        between the given start and end times inclusive, for the local MRT
        archive type. This function is agnostics of MRT archive type.
        """
        if (not start_date or not end_date):
            raise ValueError(
                f"Missing required options: start_date={start_date}, "
                f"end_date={end_date}"
            )

        start = datetime.datetime.strptime(start_date, cfg.TIME_FORMAT)
        end = datetime.datetime.strptime(end_date, cfg.TIME_FORMAT)

        if end < start:
            raise ValueError(
                f"End date {end_date} is before start date {start_date}"
            )

        diff = end - start
        mins = int(diff.total_seconds() // 60)
        filenames = []
        for i in range(0, mins + 2):
            delta = datetime.timedelta(minutes=(i * 1))
            ymd_hm = datetime.datetime.strftime(start + delta, cfg.TIME_FORMAT)
            hm = int(ymd_hm.split(".")[1][:2])*60
            hm += int(ymd_hm.split(".")[1][2:])
            if (hm % self.RIB_INTERVAL == 0):
                filenames.append(self.gen_rib_fn_date(ymd_hm))

        return filenames

    def gen_rib_key(self, ymd: str = None) -> str:
        """
        Generate the redis DB key used to store RIB stats for this
        archive, on a specific day.
        """
        if not ymd:
            raise ValueError(
                f"Missing required arguments: ymd={ymd}"
            )

        self.valid_ymd(ymd)

        return self.RIB_KEY + ":" + ymd

    def gen_rib_url(self, filename: str = None) -> str:
        """
        Generate the URL for a RIB MRT dump, based on the given MRT file name,
        for the local MRT archive type.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        if self.TYPE == "RIPE":
            return self.gen_rib_url_ripe(filename)
        elif self.TYPE == "RV":
            return self.gen_rib_url_rv(filename)
        elif self.TYPE == "AS57355":
            return self.gen_rib_url_as57355(filename)
        else:
            raise ValueError(f"Unknown MRT archive type {self.TYPE}")

    def gen_rib_url_as57355(self, filename: str = None) -> str:
        """
        Generate the URL for a given RIB MRT file, for an AS57355 MRT archive.
        """
        if not filename:
            raise ValueError(
                f"Missing required options: filename{filename}"
            )

        if filename[0:len(self.RIB_PREFIX)] != self.RIB_PREFIX:
            raise ValueError(
                f"MRT file prefix {filename[0:len(self.RIB_PREFIX)]} "
                f"is not {self.RIB_PREFIX}"
            )

        ym = filename.split(".")[1][0:6]
        ymd_hm = '.'.join(filename.split(".")[1:3])

        mrt_archive.valid_ym(ym)
        mrt_archive.valid_ymd_hm(ymd_hm)

        if filename.split(".")[-1] != self.MRT_EXT:
            raise ValueError(
                f"MRT file extension {filename.split('.')[-1]} "
                f"is not {self.MRT_EXT}"
            )

        return mrt_archive.concat_url(
            [self.BASE_URL, "/", self.RIB_URL, "/", filename]
        )

    def gen_rib_url_range(self, end_date: str = None, start_date: str = None) -> List[str]:
        """
        Generate and return a list of URLs for a range of RIB MRT dumps,
        between the given start and end times inclusive, for the local MRT
        archive type. This function is agnostic of MRT archive type.
        """
        if (not start_date or not end_date):
            raise ValueError(
                f"Missing required options: start_date={start_date}, "
                f"end_date={end_date}"
            )

        start = datetime.datetime.strptime(start_date, cfg.TIME_FORMAT)
        end = datetime.datetime.strptime(end_date, cfg.TIME_FORMAT)

        if end < start:
            raise ValueError(
                f"End date {end_date} is before start date {start_date}"
            )

        urls = []
        filenames = self.gen_rib_fns_range(end_date=end_date, start_date=start_date)
        if not filenames:
            return urls

        for filename in filenames:
            urls.append(self.gen_rib_url(filename))

        return urls

    def gen_rib_url_ripe(self, filename: str = None) -> str:
        """
        Generate the URL for a given RIB MRT file, for a RIPE MRT archive.
        """
        if not filename:
            raise ValueError(
                f"Missing required options: filename{filename}"
            )

        if filename[0:len(self.RIB_PREFIX)] != self.RIB_PREFIX:
            raise ValueError(
                f"MRT file prefix {filename[0:len(self.RIB_PREFIX)]} "
                f"is not {self.RIB_PREFIX}"
            )

        ym = filename.split(".")[1][0:6]
        ymd_hm = '.'.join(filename.split(".")[1:3])

        mrt_archive.valid_ym(ym)
        mrt_archive.valid_ymd_hm(ymd_hm)

        if filename.split(".")[-1] != self.MRT_EXT:
            raise ValueError(
                f"MRT file extension {filename.split('.')[-1]} "
                f"is not {self.MRT_EXT}"
            )

        return mrt_archive.concat_url(
            [self.BASE_URL, ym[0:4] + "." + ym[4:] + "/", self.RIB_URL, "/", filename]
        )

    def gen_rib_url_rv(self, filename: str = None) -> str:
        """
        Generate the URL for a given RIB MRT file, from a route-views MRT
        archive.
        """
        if not filename:
            raise ValueError(
                f"Missing required options: filename{filename}"
            )

        if filename[0:len(self.RIB_PREFIX)] != self.RIB_PREFIX:
            raise ValueError(
                f"MRT file prefix {filename[0:len(self.RIB_PREFIX)]} "
                f"is not {self.RIB_PREFIX}"
            )

        ym = filename.split(".")[1][0:6]
        ymd_hm = '.'.join(filename.split(".")[1:3])

        mrt_archive.valid_ym(ym)
        mrt_archive.valid_ymd_hm(ymd_hm)

        if filename.split(".")[-1] != self.MRT_EXT:
            raise ValueError(
                f"MRT file extension {filename.split('.')[-1]} "
                f"is not {self.MRT_EXT}"
            )

        y = ym[0:4]
        m = ym[4:]

        return mrt_archive.concat_url(
            [self.BASE_URL, y + "." + m + "/", self.RIB_URL, "/", filename]
        )

    def gen_upd_fn_date(self, ymd_hm: str = None) -> str:
        """
        Generate the filename of an UPDATE MRT file, for the given date and time.
        This is MRT archive type agnostic.
        """
        if not ymd_hm:
            raise ValueError(
                f"Missing required arguments: ymd_hm={ymd_hm}"
            )
        mrt_archive.valid_ymd_hm(ymd_hm)
        return f"{self.UPD_PREFIX}{ymd_hm}.{self.MRT_EXT}"

    def gen_upd_fns_day(self, ymd: str = None) -> List[str]:
        """
        Generate a list of all the UPDATE MRT filename for a specific day, for
        a specific MRT archive. This function is MRT archive type agnostic.
        """
        if not ymd:
            raise ValueError(
                f"Missing required arguments: ymd={ymd}"
            )

        mrt_archive.valid_ymd(ymd)

        filenames = []
        minutes = 0
        while(minutes < 1440):
            datetime.timedelta(minutes=minutes)
            hh = f"{minutes//60:02}"
            mm = f"{minutes%60:02}"
            filenames.append(f"{self.UPD_PREFIX}{ymd}.{hh}{mm}.{self.MRT_EXT}")
            minutes += self.UPD_INTERVAL
        return filenames

    def gen_upd_fns_range(self, end_date: str = None, start_date: str = None) -> List[str]:
        """
        Generate and return a list of filenames for a range of UPDATE MRT dumps,
        between the given start and end times inclusive, for the local MRT
        archive type. This function is agnostics of MRT archive type.
        """
        if (not start_date or not end_date):
            raise ValueError(
                f"Missing required options: start_date={start_date}, "
                f"end_date={end_date}"
            )

        start = datetime.datetime.strptime(start_date, cfg.TIME_FORMAT)
        end = datetime.datetime.strptime(end_date, cfg.TIME_FORMAT)

        if end < start:
            raise ValueError(
                f"End date {end_date} is before start date {start_date}"
            )

        diff = end - start
        mins = int(diff.total_seconds() // 60)
        filenames = []
        for i in range(0, mins + 2):
            delta = datetime.timedelta(minutes=(i * 1))
            ymd_hm = datetime.datetime.strftime(start + delta, cfg.TIME_FORMAT)
            hm = int(ymd_hm.split(".")[1][:2])*60
            hm += int(ymd_hm.split(".")[1][2:])
            if (hm % self.UPD_INTERVAL == 0):
                filenames.append(self.gen_upd_fn_date(ymd_hm))

        return filenames

    def gen_upd_key(self, ymd: str = None) -> str:
        """
        Generate the redis DB key used to store update stats for this
        archive, on a specific day.
        """
        if not ymd:
            raise ValueError(
                f"Missing required arguments: ymd={ymd}"
            )

        if type(ymd) != str:
            raise TypeError(
                f"Year, month and day value is not of type string: {type(ymd)}"
            )

        self.valid_ymd(ymd)

        return self.UPD_KEY + ":" + ymd

    def gen_upd_url(self, filename: str = None) -> str:
        """
        Generate the URL from a given update MRT file, for a specific MRT
        archive.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        if self.TYPE == "RIPE":
            return self.gen_upd_url_ripe(filename)
        elif self.TYPE == "RV":
            return self.gen_upd_url_rv(filename)
        elif self.TYPE == "AS57355":
            return self.gen_upd_url_as57355(filename)
        else:
            raise ValueError(f"Unknown MRT archive type {self.TYPE}")

    def gen_upd_url_as57355(self, filename: str = None) -> str:
        """
        Return the URL from a given UPDATE MRT filename, from an AS57355 MRT
        archive.
        """
        if not filename:
            raise ValueError(
                f"Missing required options: filename{filename}"
            )

        if filename[0:len(self.UPD_PREFIX)] != self.UPD_PREFIX:
            raise ValueError(
                f"MRT file prefix {filename[0:len(self.UPD_PREFIX)]} "
                f"is not {self.UPD_PREFIX}"
            )

        ym = filename.split(".")[0][0:6]
        ymd_hm = '.'.join(filename.split(".")[0:2])

        mrt_archive.valid_ym(ym)
        mrt_archive.valid_ymd_hm(ymd_hm)

        if filename.split(".")[-1] != self.MRT_EXT:
            raise ValueError(
                f"MRT file extension {filename.split('.')[-1]} "
                f"is not {self.MRT_EXT}"
            )

        y = ym[0:4]
        m = ym[4:]

        return mrt_archive.concat_url(
            [self.BASE_URL, "/", self.UPD_URL, "/", filename]
        )

    def gen_upd_url_range(self, end_date: str = None, start_date: str = None) -> List[str]:
        """
        Generate a and return a list of URLs for a range of UPDATE MRT dumps,
        between the given start and end times inclusive, for the local MRT
        archive type. This function is archive type agnostic.
        """
        if (not start_date or not end_date):
            raise ValueError(
                f"Missing required options: start_date={start_date}, "
                f"end_date={end_date}"
            )

        start = datetime.datetime.strptime(start_date, cfg.TIME_FORMAT)
        end = datetime.datetime.strptime(end_date, cfg.TIME_FORMAT)

        if end < start:
            raise ValueError(
                f"End date {end_date} is before start date {start_date}"
            )

        urls = []
        filenames = self.gen_upd_fns_range(end_date=end_date, start_date=start_date)
        if not filenames:
            return urls

        for filename in filenames:
            urls.append(self.gen_upd_url(filename))

        return urls
        
    def gen_upd_url_ripe(self, filename: str = None) -> str:
        """
        Generate the URL from a given UPDATE MRT filename, for a RIPE MRT
        archive.
        """
        if not filename:
            raise ValueError(
                f"Missing required options: filename{filename}"
            )

        if filename[0:len(self.UPD_PREFIX)] != self.UPD_PREFIX:
            raise ValueError(
                f"MRT file prefix {filename[0:len(self.UPD_PREFIX)]} "
                f"is not {self.UPD_PREFIX}"
            )

        ym = filename.split(".")[1][0:6]
        ymd_hm = '.'.join(filename.split(".")[1:3])

        mrt_archive.valid_ym(ym)
        mrt_archive.valid_ymd_hm(ymd_hm)

        if filename.split(".")[-1] != self.MRT_EXT:
            raise ValueError(
                f"MRT file extension {filename.split('.')[-1]} "
                f"is not {self.MRT_EXT}"
            )

        return mrt_archive.concat_url(
            [self.BASE_URL, ym[0:4] + "." + ym[4:] + "/", self.UPD_URL, filename]
        )

    def gen_upd_url_rv(self, filename: str = None) -> str:
        """
        Return the URL from a given UPDATE MRT filename, from a route-views MRT
        archive.
        """
        if not filename:
            raise ValueError(
                f"Missing required options: filename{filename}"
            )

        if filename[0:len(self.UPD_PREFIX)] != self.UPD_PREFIX:
            raise ValueError(
                f"MRT file prefix {filename[0:len(self.UPD_PREFIX)]} "
                f"is not {self.UPD_PREFIX}"
            )

        ym = filename.split(".")[1][0:6]
        ymd_hm = '.'.join(filename.split(".")[1:3])

        mrt_archive.valid_ym(ym)
        mrt_archive.valid_ymd_hm(ymd_hm)

        if filename.split(".")[-1] != self.MRT_EXT:
            raise ValueError(
                f"MRT file extension {filename.split('.')[-1]} "
                f"is not {self.MRT_EXT}"
            )

        y = ym[0:4]
        m = ym[4:]

        return mrt_archive.concat_url(
            [self.BASE_URL, y + "." + m + "/", self.UPD_URL, filename]
        )

    def ts_from_filename(self, filename: str = None) -> datetime.datetime:
        """
        Extract the ymd.hm timestamp from an MRT filename and return it.
        This function is MRT archive type agnostic.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        if self.TYPE == "RIPE":
            return self.ts_from_filename_ripe(filename)
        elif self.TYPE == "RV":
            return self.ts_from_filename_rv(filename)
        elif self.TYPE == "AS57355":
            return self.ts_from_filename_as57355(filename)
        else:
            raise ValueError(f"Unknown MRT archive type {self.TYPE}")

    def ts_from_filename_as57355(self, filename: str = None) -> datetime.datetime:
        """
        Extract the ymd.hm timestamp from an MRT filename and return it.
        This function is specific to an AS57355 MRT file.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        raw_ts = '.'.join(os.path.basename(filename).split(".")[0:2])
        return datetime.datetime.strptime(raw_ts, cfg.TIME_FORMAT)

    def ts_from_filename_ripe(self, filename: str = None) -> datetime.datetime:
        """
        Extract the ymd.hm timestamp from an MRT filename and return it.
        This function is specific to a RIPE MRT file.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        raw_ts = '.'.join(os.path.basename(filename).split(".")[1:3])
        return datetime.datetime.strptime(raw_ts, cfg.TIME_FORMAT)

    def ts_from_filename_rv(self, filename: str = None) -> datetime.datetime:
        """
        Extract the ymd.hm timestamp from an MRT filename and return it.
        This function is specific to a Route-Views MRT file.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        raw_ts = '.'.join(os.path.basename(filename).split(".")[1:3])
        return datetime.datetime.strptime(raw_ts, cfg.TIME_FORMAT)

    @staticmethod
    def valid_ym(ym: str = None):
        """
        Check if the ym string is correctly formated.
        Must be "yyyymm" e.g., "202201".
        """
        if not ym:
            raise ValueError(
                f"Missing required arguments: ym={ym}"
            )

        if type(ym) != str:
            raise TypeError(
                f"Year and month value is not of type string: {type(ym)}"
            )

        """
        No MRTs available from before 1999, and I assume this conde won't be
        running in 2030, I'm a realist :(
        """
        if not re.match(
            "^(1999|20[0-2][0-9])(0[1-9]|1[0-2])$", ym
        ):
            raise ValueError(
                f"Invalid year and month format: {ym}. "
                "Must be yyyymm e.g., 202201."
            )

    @staticmethod
    def valid_ymd(ymd: str = None):
        """
        Check if the ymd string is correctly formated.
        Must be "yyyymm" e.g., "20220101".
        """
        if not ymd:
            raise ValueError(
                f"Missing required arguments: ymd={ymd}"
            )

        if type(ymd) != str:
            raise TypeError(
                f"Year, month and day value is not of type string: {type(ymd)}"
            )

        """
        No MRTs available from before 1999, and I assume this code won't be
        running in 2030, I'm a realist :(
        """
        if not re.match(
            "^(1999|20[0-2][0-9])(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])$",
            ymd
        ):
            raise ValueError(
                f"Invalid year, month, day format: {ymd}. "
                "Must be yyyymmdd e.g., 20220110."
            )

    @staticmethod
    def valid_ymd_hm(ymd_hm: str = None):
        """
        Check if the ymd_hm string is correctly formated.
        Must be "yyyymm" e.g., "20220101.1000".
        """
        if not ymd_hm:
            raise ValueError(
                f"Missing required arguments: ymd_hm={ymd_hm}"
            )

        if type(ymd_hm) != str:
            raise TypeError(
                f"Year, month, day, hour, minute value is not of type string: "
                f"{type(ymd_hm)}"
            )

        """
        No MRTs available from before 1999, and I assume this code won't be
        running in 2030, I'm a realist :(
        """

        if not re.match(
            ("^(1999|20[0-2][0-9])(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])\."
            "([0-1][0-9]|2[0-3])([0-5][0-9])$"), ymd_hm
        ):
            raise ValueError(
                f"Invalid year, month, day, hour, minute format: {ymd_hm}. "
                "Must be yyyymmdd.hhmm e.g., 20220115.1045."
            )

    def ymd_from_file_path(self, file_path: str = None) -> str:
        """
        Return the ymd from the filename.
        """
        if not file_path:
            raise ValueError(
                f"Missing required arguments: file_path={file_path}"
            )

        if type(file_path) != str:
            raise TypeError(
                f"file_path is not of type string: {type(file_path)}"
            )

        if (self.TYPE == "RV" or self.TYPE == "RIPE"):
            ymd = os.path.basename(file_path).split(".")[1]
        elif self.TYPE == "AS57355":
            ymd = os.path.basename(file_path).split(".")[0]
        else:
            raise ValueError(f"Couldn't infer ymd from file {file_path}")

        self.valid_ymd(ymd)
        return ymd
