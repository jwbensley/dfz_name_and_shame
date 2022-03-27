import logging
import os
from typing import Union, Literal

from dnas.config import config as cfg
from dnas.mrt_getter import mrt_getter
from dnas.mrt_archive import mrt_archive

class mrt_archives:

    def __init__(self) -> None:
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
                )
            )

    def arch_from_file_path(self, file_path: str = None) -> Union['mrt_archive', Literal[False]]:
        """
        Return the MRT archive the file came from, based on the file path.
        """
        if not file_path:
            raise ValueError(
                f"Missing required arguments: file_path={file_path}."
            )

        for arch in self.archives:
            if (os.path.normpath(os.path.dirname(file_path)) == 
                os.path.normpath(arch.MRT_DIR)):
                logging.debug(f"Assuming file is from {arch.NAME} archive")
                return arch
        logging.error(f"Couldn't match {file_path} to any MRT archive")
        return False

    def arch_from_url(self, url: str = None) -> Union['mrt_archive', Literal[False]]:
        """
        Return the MRT archive the URL belongs to, based on the url.
        """
        if not url:
            raise ValueError(
                f"Missing required arguments: url={url}."
            )

        for arch in self.archives:
            if self.is_rib_from_filename(os.path.basename(url)):
                try:
                    if arch.gen_rib_url(os.path.basename(url)) == url:
                        logging.debug(f"Assuming URL is from {arch.NAME} archive")
                        return arch
                except ValueError:
                    """
                    If this is a route-views file for example,
                    trying to generate a RIPE URL will raise ValueError
                    """
                    pass
            else:
                try:
                    if arch.gen_upd_url(os.path.basename(url)) == url:
                        logging.debug(f"Assuming URL is from {arch.NAME} archive")
                        return arch
                except ValueError:
                    pass
        logging.error(f"Couldn't match {url} to any MRT archive")
        return False

    def get_day_key(self, file_path: str = None) -> str:
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
        if not arch:
            raise ValueError(
                f"Unable to determine MRT archive from file path {file_path}"
            )

        ymd = arch.ymd_from_file_path(file_path)

        if self.is_rib_from_filename(file_path):
            return arch.gen_rib_key(ymd)
        else:
            return arch.gen_upd_key(ymd)

    def is_rib_from_filename(self, file_path: str = None) -> bool:
        """
        Return True if this is a RIB dump, else False to indicate UPDATE dump.
        """
        if not file_path:
            raise ValueError(
                f"Missing required arguments: file_path={file_path}."
            )

        filename = os.path.basename(file_path)
        for arch in self.archives:
            if arch.RIB_PREFIX in filename:
                return True
        return False
