class mrt_archive:

    def __init__(
        self,
        BASE_URL=None,
        ENABLED=False,
        MRT_DIR=None,
        MRT_EXT=None,
        NAME=None,
        RIB_GLOB=None,
        RIB_KEY=None,
        RIB_URL=None,
        TYPE=None,
        UPD_GLOB=None,
        UPD_KEY=None,
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
        self.RIB_KEY = RIB_KEY
        self.RIB_URL = RIB_URL
        self.TYPE = TYPE
        self.UPD_GLOB = UPD_GLOB
        self.UPD_KEY = UPD_KEY
        self.UPD_URL = UPD_URL
        self.get_latest_rib = get_latest_rib
        self.get_latest_upd = get_latest_upd
        self.get_range_rib = get_range_rib
        self.get_range_upd = get_range_upd
