from dnas.config import config as cfg
from dnas.mrt_getter import mrt_getter
from dnas.mrt_archive import mrt_archive

class mrt_archives:

    def __init__(self):
        self.archives = []

        for arch in cfg.MRT_ARCHIVES:
            self.archives.append(
                mrt_archive(
                    BASE_URL = arch["BASE_URL"],
                    ENABLED = arch["ENABLED"],
                    MRT_DIR = arch["MRT_DIR"],
                    MRT_EXT = arch["MRT_EXT"],
                    NAME = arch["NAME"],
                    RIB_GLOB = arch["RIB_GLOB"],
                    RIB_KEY = arch["RIB_KEY"],
                    RIB_URL = arch["RIB_URL"],
                    TYPE = arch["TYPE"],
                    UPD_GLOB = arch["UPD_GLOB"],
                    UPD_KEY = arch["UPD_KEY"],
                    UPD_URL = arch["UPD_URL"],
                    get_latest_rib = mrt_getter.get_rv_latest_rib if (arch["TYPE"] == "RV") else mrt_getter.get_ripe_latest_rib,
                    get_latest_upd = mrt_getter.get_rv_latest_upd if (arch["TYPE"] == "RV") else mrt_getter.get_ripe_latest_upd,
                    get_range_rib = mrt_getter.get_rv_range_rib if (arch["TYPE"] == "RV") else mrt_getter.get_ripe_range_rib,
                    get_range_upd = mrt_getter.get_rv_range_upd if (arch["TYPE"] == "RV") else mrt_getter.get_ripe_range_upd,
                )
            )
