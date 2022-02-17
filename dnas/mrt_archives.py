import logging
import os

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
                    RIB_INTERVAL = arch["RIB_INTERVAL"],
                    RIB_KEY = arch["RIB_KEY"],
                    RIB_PREFIX = arch["RIB_PREFIX"],
                    RIB_URL = arch["RIB_URL"],
                    TYPE = arch["TYPE"],
                    UPD_GLOB = arch["UPD_GLOB"],
                    UPD_INTERVAL = arch["UPD_INTERVAL"],
                    UPD_KEY = arch["UPD_KEY"],
                    UPD_PREFIX = arch["UPD_PREFIX"],
                    UPD_URL = arch["UPD_URL"],
                    get_latest_rib = mrt_getter.get_rv_latest_rib if (arch["TYPE"] == "RV") else mrt_getter.get_ripe_latest_rib,
                    get_latest_upd = mrt_getter.get_rv_latest_upd if (arch["TYPE"] == "RV") else mrt_getter.get_ripe_latest_upd,
                    get_range_rib = mrt_getter.get_rv_range_rib if (arch["TYPE"] == "RV") else mrt_getter.get_ripe_range_rib,
                    get_range_upd = mrt_getter.get_rv_range_upd if (arch["TYPE"] == "RV") else mrt_getter.get_ripe_range_upd,
                )
            )

    def arch_from_file_path(self, file_path):
        """
        Return the MRT archive the file came from, based on the file path
        """
        if not file_path:
            raise ValueError(
                f"Missing required arguments: file_path={file_path}."
            )

        for arch in self.archives:
            if os.path.dirname(file_path) == arch.MRT_DIR:
                logging.debug(f"Assuming file is from {arch.NAME} archive")
                return arch
        logging.error(f"Couldn't match {file_path} to any MRT archive")
        return False

    def get_day_key(self, file_path):
        """
        Return the redis DB key for the specific MRT archive and specific day
        the file path relates to.
        """
        if not file_path:
            raise ValueError(
                f"Missing required arguments: file_path={file_path}."
            )

        # Example: /path/to/route-views/LINX/updates.20220101.0600.bz2
        arch = self.arch_from_file_path(file_path)
        day = file.split(".")[1]
        if self.is_rib_from_filename(file_path):
            return arch.RIB_KEY + ":" + day
        else:
            return arch.UPD_KEY + ":" + day

    def is_rib_from_filename(self, file_path):
        """
        Return True if this is a RIB dump, else False to indicate UPDATE dump.
        """
        if not file_path:
            raise ValueError(
                f"Missing required arguments: file_path={file_path}."
            )

        filename = os.basename(file_path)

        is_rib = False
        for arch in self.archives:
            if arch.RIB_PREFIX in filename:
                is_rib = True
                break
        return is_rib
