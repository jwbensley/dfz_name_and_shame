import datetime
import re

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
        get_latest_rib=None,
        get_latest_upd=None,
        get_range_rib=None,
        get_range_upd=None,
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
        self.get_latest_rib = get_latest_rib
        self.get_latest_upd = get_latest_upd
        self.get_range_rib = get_range_rib
        self.get_range_upd = get_range_upd

    def gen_rib_filenames(self, ymd):
        """
        Return a list of all the RIB MRT files for a specific day.
        """

        """
        No MRTs available from before 1999, and I assume this conde won't be
        running in 2030, I'm a realist :(
        """
        if not re.match(
            "(1999|20[0-2][0-9])(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])", ymd
        ):
            raise ValueError(
                f"Invalid year, month, day format: {ymd}. "
                "Must be yyyymmdd e.g., 20220110"
            )

        filenames = []
        minutes = 0
        while(minutes < 1440):
            datetime.timedelta(minutes=minutes)
            hh = f"{minutes//60:02}"
            mm = f"{minutes%60:02}"
            filenames.append(f"{self.RIB_PREFIX}{ymd}.{hh}{mm}.{self.MRT_EXT}")
            minutes += self.RIB_INTERVAL
        return filenames

    def gen_rib_url(self, filename):
        if self.TYPE == "RIPE":
            return self.gen_rib_ripe_url(filename)
        elif self.TYPE == "RV":
            return self.gen_rib_rv_url(filename)
        else:
            raise ValueError(f"Unknown MRT archive type {self.TYPE}")

    def gen_rib_ripe_url(self, filename):
        """
        Return the URL for a specifc RIB MRT file from a RIPE MRT archive.
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

        """
        No MRTs available from before 1999, and I assume this conde won't be
        running in 2030, I'm a realist :(
        """
        if not re.match(
            "(1999|20[0-2][0-9])(0[1-9]|1[0-2])", ym
        ):
            raise ValueError(
                f"Invalid year and month format for filename: {filename}. "
                "Must be yyyymm e.g., 202201"
            )
            exit(1)

        if not re.match(
            "(1999|20[0-2][0-9])(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])\.([0-1][0-9]|2[0-3])([0-5][0-9])", ymd_hm
        ):
            raise ValueError(
                f"Invalid year, month, day, hour, minute format for filename: "
                f"{filename}. Must be yyyymmdd.hhmm e.g., 20220115.1045"
            )
            exit(1)

        if filename.split(".")[-1] != self.MRT_EXT:
            raise ValueError(
                f"MRT file extension {filename.split('.')[-1]} "
                f"is not {self.MRT_EXT}"
            )

        y = ym[0:5]
        m = ym[4:]
        url = self.BASE_URL + ym + self.RIB_URL + filename

    def gen_rib_rv_url(self, filename):
        """
        Return the URL for a specifc RIB MRT file from a route-views MRT archive.
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

        """
        No MRTs available from before 1999, and I assume this conde won't be
        running in 2030, I'm a realist :(
        """
        if not re.match(
            "(1999|20[0-2][0-9])(0[1-9]|1[0-2])", ym
        ):
            raise ValueError(
                f"Invalid year and month format for filename: {filename}. "
                "Must be yyyymm e.g., 202201"
            )
            exit(1)

        if not re.match(
            "(1999|20[0-2][0-9])(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])\.([0-1][0-9]|2[0-3])([0-5][0-9])", ymd_hm
        ):
            raise ValueError(
                f"Invalid year, month, day, hour, minute format for filename: "
                f"{filename}. Must be yyyymmdd.hhmm e.g., 20220115.1045"
            )
            exit(1)

        if filename.split(".")[-1] != self.MRT_EXT:
            raise ValueError(
                f"MRT file extension {filename.split('.')[-1]} "
                f"is not {self.MRT_EXT}"
            )

        y = ym[0:5]
        m = ym[4:]
        return self.BASE_URL + y + "." + m + self.RIB_URL + filename

    def gen_upd_filenames(self, ymd):
        """
        Return a list of all the RIB MRT files for a specific day.
        """

        """
        No MRTs available from before 1999, and I assume this conde won't be
        running in 2030, I'm a realist :(
        """
        if not re.match(
            "(1999|20[0-2][0-9])(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])", ymd
        ):
            raise ValueError(
                f"Invalid year, month, day format: {ymd}. "
                "Must be yyyymmdd e.g., 20220110"
            )

        filenames = []
        minutes = 0
        while(minutes < 1440):
            datetime.timedelta(minutes=minutes)
            hh = f"{minutes//60:02}"
            mm = f"{minutes%60:02}"
            filenames.append(f"{self.UPD_PREFIX}{ymd}.{hh}{mm}.{self.MRT_EXT}")
            minutes += self.UPD_INTERVAL
        return filenames

    def gen_upd_url(self, filename):
        if self.TYPE == "RIPE":
            return self.gen_upd_ripe_url(filename)
        elif self.TYPE == "RV":
            return self.gen_upd_rv_url(filename)
        else:
            raise ValueError(f"Unknown MRT archive type {self.TYPE}")

    def get_upd_ripe_url(self, filename):
        """
        Return the URL for a specifc UPDATE MRT file from a RIPE MRT archive.
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

        """
        No MRTs available from before 1999, and I assume this conde won't be
        running in 2030, I'm a realist :(
        """
        if not re.match(
            "(1999|20[0-2][0-9])(0[1-9]|1[0-2])", ym
        ):
            raise ValueError(
                f"Invalid year and month format for filename: {filename}. "
                "Must be yyyymm e.g., 202201"
            )
            exit(1)

        if not re.match(
            "(1999|20[0-2][0-9])(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])\.([0-1][0-9]|2[0-3])([0-5][0-9])", ymd_hm
        ):
            raise ValueError(
                f"Invalid year, month, day, hour, minute format for filename: "
                f"{filename}. Must be yyyymmdd.hhmm e.g., 20220115.1045"
            )
            exit(1)

        if filename.split(".")[-1] != self.MRT_EXT:
            raise ValueError(
                f"MRT file extension {filename.split('.')[-1]} "
                f"is not {self.MRT_EXT}"
            )

        y = ym[0:5]
        m = ym[4:]
        url = self.BASE_URL + ym + self.UPD_URL + filename

    def gen_upd_rv_url(self, filename):
        """
        Return the URL for a specifc UPDATE MRT file from a route-views MRT archive.
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

        """
        No MRTs available from before 1999, and I assume this conde won't be
        running in 2030, I'm a realist :(
        """
        if not re.match(
            "(1999|20[0-2][0-9])(0[1-9]|1[0-2])", ym
        ):
            raise ValueError(
                f"Invalid year and month format for filename: {filename}. "
                "Must be yyyymm e.g., 202201"
            )
            exit(1)

        if not re.match(
            "(1999|20[0-2][0-9])(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])\.([0-1][0-9]|2[0-3])([0-5][0-9])", ymd_hm
        ):
            raise ValueError(
                f"Invalid year, month, day, hour, minute format for filename: "
                f"{filename}. Must be yyyymmdd.hhmm e.g., 20220115.1045"
            )
            exit(1)

        if filename.split(".")[-1] != self.MRT_EXT:
            raise ValueError(
                f"MRT file extension {filename.split('.')[-1]} "
                f"is not {self.MRT_EXT}"
            )

        y = ym[0:5]
        m = ym[4:]
        return self.BASE_URL + y + "." + m + self.UPD_URL + filename
