from functools import reduce
import datetime
import os
import re
import urllib.parse

class mrt_archive:

    def __init__(
        self,
        BASE_URL=None,
        ENABLED=False,
        MRT_DIR=None,
        MRT_EXT=None,
        NAME=None,
        RIB_GLOB=None,
        RIB_INTERVAL=None,
        RIB_KEY=None,
        RIB_PREFIX=None,
        RIB_URL=None,
        TYPE=None,
        UPD_GLOB=None,
        UPD_INTERVAL=None,
        UPD_KEY=None,
        UPD_PREFIX=None,
        UPD_URL=None,
        get_rib_url=None,
        get_upd_url=None,

    ):

        self.BASE_URL = BASE_URL
        self.ENABLED = ENABLED
        self.MRT_DIR = MRT_DIR
        self.MRT_EXT = MRT_EXT
        self.NAME = NAME
        self.RIB_GLOB = RIB_GLOB
        self.RIB_INTERVAL = RIB_INTERVAL
        self.RIB_KEY = RIB_KEY
        self.RIB_PREFIX = RIB_PREFIX
        self.RIB_URL = RIB_URL
        self.TYPE = TYPE
        self.UPD_GLOB = UPD_GLOB
        self.UPD_INTERVAL = UPD_INTERVAL
        self.UPD_KEY = UPD_KEY
        self.UPD_PREFIX = UPD_PREFIX
        self.UPD_URL = UPD_URL

    @staticmethod
    def concat_url(url_chunks):
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

        return reduce(
            urllib.parse.urljoin,
            map(lambda x : x.lstrip("/"), url_chunks)
        )

    def gen_latest_rib_fn(self, filename):
        """
        Generate and return the filename for the newest/most recent RIB dump
        from a this object's archive.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        if self.TYPE == "RIPE":
            return self.gen_latest_rib_fn_ripe(filename)
        elif self.TYPE == "RV":
            return self.gen_latest_rib_fn_rv(filename)
        else:
            raise ValueError(f"Unknown MRT archive type {self.TYPE}")

    def gen_latest_rib_fn_ripe(self):
        """
        Generate and return the filename for the newest/most recent RIB dump
        from a RIPE MRT archive.

        RIPE RIB dumps are every 8 hours.

        When downloading rib dumps from RIPE, calculate the time of the last
        whole 8 hour interval. RIPE RIB files are dumped every 8 hours, if it's
        09.00 we're half way through the 08.00 to 16.00 time period,
        which will be available from 16.00 onwards. This means that the latest
        complete RIB dump we can download at 09.00 would be from the 00.00-08.00
        period and it would be called "0000". h_delta gets us back to 00.00.

        If this machine is in a different timezone to the archive server, an
        additional offset is required, RIPE_RIB_OFFSET.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        hours = int(datetime.datetime.strftime(datetime.datetime.now(), "%H"))
        mod = hours % (self.RIB_INTERVAL // 60)
        if mod == 0:
            h_delta = datetime.timedelta(hours=(self.RIB_INTERVAL // 60))
        else:
            h_delta = datetime.timedelta(hours=((self.RIB_INTERVAL // 60) + mod))

        ymd_hm = datetime.datetime.strftime(
            datetime.datetime.now() - h_delta,
            "%Y%m%d.%H00"
        )

        return self.RIB_PREFIX + ymd_hm + "." + self.MRT_EXT

    def gen_latest_rib_fn_rv(self):
        """
        Generate and return the filename for the newest/most recent RIB dump
        from a route-views MRT archive.

        Route-views RIB dumps are every 2 hours.

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
        if hours % (self.RIB_INTERVAL // 60) != 0:
            hours = ((self.RIB_INTERVAL // 60) + 1) + cfg.RV_RIB_OFFSET
        else:
            hours = (self.RIB_INTERVAL // 60) + cfg.RV_RIB_OFFSET

        h_delta = datetime.timedelta(hours=hours)
        ymd_hm = datetime.datetime.strftime(
            datetime.datetime.now() - h_delta,
            "%Y%m%d.%H00"
        )

        return self.RIB_PREFIX + ymd_hm + "." + self.MRT_EXT

    def gen_latest_upd_fn(self, filename):
        """
        Generate and return the filename for the newest/most recent UPDATE dump
        from a this object's archive.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}"
            )

        if self.TYPE == "RIPE":
            return self.gen_latest_upd_fn_ripe(filename)
        elif self.TYPE == "RV":
            return self.gen_latest_upd_fn_rv(filename)
        else:
            raise ValueError(f"Unknown MRT archive type {self.TYPE}")

    def gen_latest_upd_fn_ripe(self):
        """
        Generate and return the filename of the newest/most recent UPDATE dump
        for a RIPE MRT archive.

        RIPE UPDATE dumps are every 5 minutes.

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
        mod = minutes % self.UPD_INTERVAL
        if mod == 0:
            m_delta = datetime.timedelta(minutes=(self.UPD_INTERVAL* 2))
        else:
            m_delta = datetime.timedelta(minutes=(self.UPD_INTERVAL + mod))

        h_delta = datetime.timedelta(hours=cfg.RCC_UPD_OFFSET)

        ymd_hm = datetime.datetime.strftime(
            datetime.datetime.now() - h_delta - m_delta,
            cfg.TIME_FORMAT
        )

        return self.UPD_PREFIX + ymd_hm + "." + self.MRT_EXT

    def gen_latest_upd_fn_rv(self):
        """
        Generate and return the filename of the newest/most recent UPDATE dump
        for a route-views MRT archive.

        Route-views UPDATE dumps are every 15 minutes.

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
        mod = minutes % self.UPD_INTERVAL
        if mod == 0:
            m_delta = datetime.timedelta(minutes=2*self.UPD_INTERVAL)
        else:
            m_delta = datetime.timedelta(minutes=(self.UPD_INTERVAL + mod))

        h_delta = datetime.timedelta(hours=cfg.RV_UPD_OFFSET)

        ####################################ym = datetime.datetime.strftime(datetime.datetime.now()-h_delta,"%Y.%m")
        ymd_hm = datetime.datetime.strftime(datetime.datetime.now()-h_delta-m_delta,"%Y%m%d.%H%M")
        return self.UPD_PREFIX + ymd_hm + "." + self.MRT_EXT

    def gen_rib_fn_date(self, ymd_hm):
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

    def gen_rib_fns_day(self, ymd):
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

    def gen_rib_fns_range(self, end_date, start_date):
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
        count = int(diff.total_seconds()) // (self.RIB_INTERVAL * 60)
        filenames = []
        for i in range(0, count + 1):
            delta = datetime.timedelta(minutes=(i * self.RIB_INTERVAL))
            ymd_hm = datetime.datetime.strftime(start + delta, cfg.TIME_FORMAT)
            filenames.append(self.gen_rib_fn_date(ymd_hm))

        return filenames

    def gen_rib_key(self, ymd):
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

    def gen_rib_url(self, filename):
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
        else:
            raise ValueError(f"Unknown MRT archive type {self.TYPE}")

    def gen_rib_url_range(self, end_date, start_date):
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

        diff = end - start
        count = int(diff.total_seconds()) // (self.RIB_INTERVAL * 60)
        url_list = []
        for i in range(0, count + 1):
            m_delta = datetime.timedelta(minutes=(i * self.RIB_INTERVAL))
            ymd_hm = datetime.datetime.strftime(start+m_delta, cfg.TIME_FORMAT)
            url_list.append(
                self.gen_rib_url(self.gen_rib_fn_date(ymd_hm))
            )

        return url_list

    def gen_rib_url_ripe(self, filename):
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

        return mrt_archive.concat_url([self.BASE_URL, ym + "/", self.RIB_URL, "/", filename])

    def gen_rib_url_rv(self, filename):
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

    def gen_upd_fn_date(self, ymd_hm):
        """
        Generate the filename of an UPDATE MRT file, for the given date and time.
        This is MRT archive type agnostic.
        """
        if not ymd_hm:
            raise ValueError(
                f"Missing required arguments: ymd_hm={ymd_hm}"
            )
        mrt_archive.valid_ymd_hm(ymd_hm)
        return f"{self.RIB_PREFIX}{ymd_hm}.{self.MRT_EXT}"

    def gen_upd_fns_day(self, ymd):
        """
        Generate a list of all the RIB MRT filename for a specific day, for
        a specific MRT archive.
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

    def gen_upd_fns_range(self, end_date, start_date):
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
        count = int(diff.total_seconds()) // (self.UPD_INTERVAL * 60)
        filenames = []
        for i in range(0, count + 1):
            delta = datetime.timedelta(minutes=(i * self.UPD_INTERVAL))
            ymd_hm = datetime.datetime.strftime(start + delta, cfg.TIME_FORMAT)
            filenames.append(self.gen_upd_fn_date(ymd_hm))

        return filenames

    def gen_upd_key(self, ymd):
        """
        Generate the redis DB key used to store update stats for this
        archive, on a specific day.
        """
        if not ymd:
            raise ValueError(
                f"Missing required arguments: ymd={ymd}"
            )

        self.valid_ymd(ymd)

        return self.UPD_KEY + ":" + ymd

    def gen_upd_url(self, filename):
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
        else:
            raise ValueError(f"Unknown MRT archive type {self.TYPE}")

    def gen_upd_url_range(self, end_date, start_date):
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

        diff = end - start
        count = int(diff.total_seconds()) // (self.UPD_INTERVAL * 60)
        url_list = []
        for i in range(0, count + 1):
            delta = datetime.timedelta(minutes=(i * self.UPD_INTERVAL))
            ymd_hm = datetime.datetime.strftime(start + delta, cfg.TIME_FORMAT)

            if self.TYPE == "RIPE":
                url_list.append(
                    self.gen_upd_url_ripe(self.gen_upd_fn_date(ymd_hm))
                )
            elif self.TYPE == "RV":
                url_list.append(
                    self.gen_upd_url_rv(self.gen_upd_fn_date(ymd_hm))
                )
            else:
                raise ValueError(f"Unknown MRT archive type {self.TYPE}")

        return url_list

    def gen_upd_url_ripe(self, filename):
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

        return mrt_archive.concat_url([self.BASE_URL, ym + "/", self.UPD_URL, filename])

    def gen_upd_url_rv(self, filename):
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

        return mrt_archive.concat_url([self.BASE_URL, y + "." + m + "/", self.UPD_URL, filename])

    @staticmethod
    def valid_ym(ym):
        """
        Check if the ym string is correctly formated.
        Must be "yyyymm" e.g., "202201".
        """
        if not ym:
            raise ValueError(
                f"Missing required arguments: ym={ym}"
            )

        """
        No MRTs available from before 1999, and I assume this conde won't be
        running in 2030, I'm a realist :(
        """
        if not re.match(
            "(1999|20[0-2][0-9])(0[1-9]|1[0-2])", ym
        ):
            raise ValueError(
                f"Invalid year and month format: {ym}. "
                "Must be yyyymm e.g., 202201."
            )

    @staticmethod
    def valid_ymd(ymd):
        """
        Check if the ymd string is correctly formated.
        Must be "yyyymm" e.g., "20220101".
        """
        if not ymd:
            raise ValueError(
                f"Missing required arguments: ymd={ymd}"
            )

        """
        No MRTs available from before 1999, and I assume this code won't be
        running in 2030, I'm a realist :(
        """
        if not re.match(
            "(1999|20[0-2][0-9])(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])", ymd
        ):
            raise ValueError(
                f"Invalid year, month, day format: {ymd}. "
                "Must be yyyymmdd e.g., 20220110."
            )

    @staticmethod
    def valid_ymd_hm(ymd_hm):
        """
        Check if the ymd_hm string is correctly formated.
        Must be "yyyymm" e.g., "20220101.1000".
        """
        if not ymd_hm:
            raise ValueError(
                f"Missing required arguments: ymd_hm={ymd_hm}"
            )

        """
        No MRTs available from before 1999, and I assume this code won't be
        running in 2030, I'm a realist :(
        """

        if not re.match(
            "(1999|20[0-2][0-9])(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])\.([0-1][0-9]|2[0-3])([0-5][0-9])", ymd_hm
        ):
            raise ValueError(
                f"Invalid year, month, day, hour, minute format: {ymd_hm}. "
                "Must be yyyymmdd.hhmm e.g., 20220115.1045."
            )
