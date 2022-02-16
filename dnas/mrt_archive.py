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

    @staticmethod
    def gen_rib_filenames(ymd):
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
            filenames.append(f"{self.RIB_PREFIX}.{ymd}.{hh}{mm}.{self.MRT_EXT}")
            minutes += self.RIB_INTERVAL
        return filenames

    @staticmethod
    def gen_upd_filenames(ymd):
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
            filenames.append(f"{self.UPD_PREFIX}.{ymd}.{hh}{mm}.{self.MRT_EXT}")
            minutes += self.UPD_INTERVAL
        return filenames
