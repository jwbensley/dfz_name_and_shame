import datetime
import os
import re
import sys
import unittest

sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)
from dnas.config import config
from dnas.mrt_archive import mrt_archive


class test_mrt_archive(unittest.TestCase):

    cfg = config()

    AS57355_TYPE = "AS57355"
    AS57355_NAME = "AS57355-Lukasz"
    AS57355_ENABLED = True
    AS57355_BASE_URL = "http://192.168.58.8:8000/lukasz/"
    AS57355_RIB_URL = "/rib/"
    AS57355_UPD_URL = "/update/"
    AS57355_MRT_EXT = "dump"
    AS57355_MRT_DIR = os.path.join(cfg.DL_DIR, "AS57355/")
    AS57355_RIB_GLOB = "*.dump"
    AS57355_UPD_GLOB = "*.dump"
    AS57355_RIB_KEY = "UNIT_TEST_AS57355_RIB"
    AS57355_UPD_KEY = "UNIT_TEST_AS57355_UPD"
    AS57355_RIB_INTERVAL = 60
    AS57355_UPD_INTERVAL = 10
    AS57355_RIB_PREFIX = "rib."
    AS57355_UPD_PREFIX =  ""
    AS57355_RIB_OFFSET = 0
    AS57355_UPD_OFFSET = 120

    mrt_as57355 = mrt_archive(
            TYPE = AS57355_TYPE,
            NAME = AS57355_NAME,
            ENABLED = AS57355_ENABLED,
            BASE_URL = AS57355_BASE_URL,
            RIB_URL = AS57355_RIB_URL,
            UPD_URL = AS57355_UPD_URL,
            MRT_EXT = AS57355_MRT_EXT,
            MRT_DIR = AS57355_MRT_DIR,
            RIB_GLOB = AS57355_RIB_GLOB,
            UPD_GLOB = AS57355_UPD_GLOB,
            RIB_KEY = AS57355_RIB_KEY,
            UPD_KEY = AS57355_UPD_KEY,
            RIB_INTERVAL = AS57355_RIB_INTERVAL,
            UPD_INTERVAL = AS57355_UPD_INTERVAL,
            RIB_PREFIX = AS57355_RIB_PREFIX,
            UPD_PREFIX = AS57355_UPD_PREFIX,
            RIB_OFFSET = AS57355_RIB_OFFSET,
            UPD_OFFSET = AS57355_UPD_OFFSET,
        )

    RV_TYPE = "RV"
    RV_NAME = "RV_LINX"
    RV_ENABLED = True
    RV_BASE_URL = "http://archive.routeviews.org/route-views.linx/bgpdata/"
    RV_RIB_URL = "/RIBS/"
    RV_UPD_URL = "/UPDATES/"
    RV_MRT_EXT = "bz2"
    RV_MRT_DIR = os.path.join(cfg.DL_DIR, "LINX/")
    RV_RIB_GLOB = "rib.*bz2"
    RV_UPD_GLOB = "updates.*bz2"
    RV_RIB_KEY = "UNIT_TEST_RV_RIB"
    RV_UPD_KEY = "UNIT_TEST_RV_UPD"
    RV_RIB_INTERVAL = 120
    RV_UPD_INTERVAL = 15
    RV_RIB_PREFIX = "rib."
    RV_UPD_PREFIX = "updates."
    RV_RIB_OFFSET = 120
    RV_UPD_OFFSET = 120

    mrt_rv = mrt_archive(
            TYPE = RV_TYPE,
            NAME = RV_NAME,
            ENABLED = RV_ENABLED,
            BASE_URL = RV_BASE_URL,
            RIB_URL = RV_RIB_URL,
            UPD_URL = RV_UPD_URL,
            MRT_EXT = RV_MRT_EXT,
            MRT_DIR = RV_MRT_DIR,
            RIB_GLOB = RV_RIB_GLOB,
            UPD_GLOB = RV_UPD_GLOB,
            RIB_KEY = RV_RIB_KEY,
            UPD_KEY = RV_UPD_KEY,
            RIB_INTERVAL = RV_RIB_INTERVAL,
            UPD_INTERVAL = RV_UPD_INTERVAL,
            RIB_PREFIX = RV_RIB_PREFIX,
            UPD_PREFIX = RV_UPD_PREFIX,
            RIB_OFFSET = RV_RIB_OFFSET,
            UPD_OFFSET = RV_UPD_OFFSET,
        )

    RIPE_TYPE = "RIPE"
    RIPE_NAME = "RCC_23"
    RIPE_ENABLED = False
    RIPE_BASE_URL = "https://data.ris.ripe.net/rrc23/"
    RIPE_RIB_URL = "/"
    RIPE_UPD_URL = "/"
    RIPE_MRT_EXT = "gz"
    RIPE_MRT_DIR = os.path.join(cfg.DL_DIR, "RCC23/")
    RIPE_RIB_GLOB = "bview.*gz"
    RIPE_UPD_GLOB = "updates.*gz"
    RIPE_RIB_KEY = "UNIT_TEST_RIPE_RIB"
    RIPE_UPD_KEY = "UNIT_TEST_RIPE_UPD"
    RIPE_RIB_INTERVAL = 480
    RIPE_UPD_INTERVAL = 5
    RIPE_RIB_PREFIX = "bview."
    RIPE_UPD_PREFIX = "updates."
    RIPE_RIB_OFFSET = 0
    RIPE_UPD_OFFSET = 120

    mrt_ripe = mrt_archive(
            TYPE = RIPE_TYPE,
            NAME = RIPE_NAME,
            ENABLED = RIPE_ENABLED,
            BASE_URL = RIPE_BASE_URL,
            RIB_URL = RIPE_RIB_URL,
            UPD_URL = RIPE_UPD_URL,
            MRT_EXT = RIPE_MRT_EXT,
            MRT_DIR = RIPE_MRT_DIR,
            RIB_GLOB = RIPE_RIB_GLOB,
            UPD_GLOB = RIPE_UPD_GLOB,
            RIB_KEY = RIPE_RIB_KEY,
            UPD_KEY = RIPE_UPD_KEY,
            RIB_INTERVAL = RIPE_RIB_INTERVAL,
            UPD_INTERVAL = RIPE_UPD_INTERVAL,
            RIB_PREFIX = RIPE_RIB_PREFIX,
            UPD_PREFIX = RIPE_UPD_PREFIX,
            RIB_OFFSET = RIPE_RIB_OFFSET,
            UPD_OFFSET = RIPE_UPD_OFFSET,
        )

    def test_init(self):

        with self.assertRaises(TypeError):
            fail = mrt_archive(
                TYPE = 123,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
        with self.assertRaises(TypeError):
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = 123,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
        with self.assertRaises(TypeError):
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = 123.456,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
        with self.assertRaises(TypeError):
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = 123,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
        with self.assertRaises(TypeError):
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = 123,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
        with self.assertRaises(TypeError):
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = 123,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
        with self.assertRaises(TypeError):
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = 123,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
        with self.assertRaises(TypeError):
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = 123,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
        with self.assertRaises(TypeError):
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = 123,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
        with self.assertRaises(TypeError):
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = 123,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
        with self.assertRaises(TypeError):
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = 123,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = 123,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = "abc",
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = "abc",
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = 123,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = 123,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = "abc",
                UPD_OFFSET = self.RIPE_UPD_OFFSET,
            )
            fail = mrt_archive(
                TYPE =  self.RIPE_TYPE,
                NAME = self.RIPE_NAME,
                ENABLED = self.RIPE_ENABLED,
                BASE_URL = self.RIPE_BASE_URL,
                RIB_URL = self.RIPE_RIB_URL,
                UPD_URL = self.RIPE_UPD_URL,
                MRT_EXT = self.RIPE_MRT_EXT,
                MRT_DIR = self.RIPE_MRT_DIR,
                RIB_GLOB = self.RIPE_RIB_GLOB,
                UPD_GLOB = self.RIPE_UPD_GLOB,
                RIB_KEY = self.RIPE_RIB_KEY,
                UPD_KEY = self.RIPE_UPD_KEY,
                RIB_INTERVAL = self.RIPE_RIB_INTERVAL,
                UPD_INTERVAL = self.RIPE_UPD_INTERVAL,
                RIB_PREFIX = self.RIPE_RIB_PREFIX,
                UPD_PREFIX = self.RIPE_UPD_PREFIX,
                RIB_OFFSET = self.RIPE_RIB_OFFSET,
                UPD_OFFSET = "abc",
            )

        self.assertIsInstance(self.mrt_rv, mrt_archive)
        self.assertIsInstance(self.mrt_rv.TYPE, str)
        self.assertIsInstance(self.mrt_rv.NAME, str)
        self.assertIsInstance(self.mrt_rv.ENABLED, bool)
        self.assertIsInstance(self.mrt_rv.BASE_URL, str)
        self.assertIsInstance(self.mrt_rv.RIB_URL, str)
        self.assertIsInstance(self.mrt_rv.UPD_URL, str)
        self.assertIsInstance(self.mrt_rv.MRT_EXT, str)
        self.assertIsInstance(self.mrt_rv.MRT_DIR, str)
        self.assertIsInstance(self.mrt_rv.RIB_GLOB, str)
        self.assertIsInstance(self.mrt_rv.UPD_GLOB, str)
        self.assertIsInstance(self.mrt_rv.RIB_KEY, str)
        self.assertIsInstance(self.mrt_rv.UPD_KEY, str)
        self.assertIsInstance(self.mrt_rv.RIB_INTERVAL, int)
        self.assertIsInstance(self.mrt_rv.UPD_INTERVAL, int)
        self.assertIsInstance(self.mrt_rv.RIB_PREFIX, str)
        self.assertIsInstance(self.mrt_rv.UPD_PREFIX, str)
        self.assertIsInstance(self.mrt_rv.RIB_OFFSET, int)
        self.assertIsInstance(self.mrt_rv.UPD_OFFSET, int)

    def test_concat_url(self):
        url = "http://www.example.tld/path/to/file.abc"
        self.assertEqual(self.mrt_ripe.concat_url(["http://www.example.tld/", "/path", "/to", "/", "file.abc"]), url)
        self.assertEqual(self.mrt_ripe.concat_url(["http://www.example.tld/", "path", "/to", "/", "file.abc"]), url)
        self.assertEqual(self.mrt_ripe.concat_url(["http://www.example.tld", "/path", "/to", "/", "file.abc"]), url)
        self.assertEqual(self.mrt_ripe.concat_url(["http://www.example.tld", "path", "/to", "/", "file.abc"]), url)
        self.assertEqual(self.mrt_ripe.concat_url(["http://www.example.tld", "path", "/to", "/", "/file.abc"]), url)

    """
    How to generate the file names based on the current time, without
    using the exact same method as the class? Can't be arsed to work this
    out. For the following group of function tests, let's just check with
    regex the return value is a valid multiple of the MRT file offset for
    either today or yesterady (in case this test is being run somewhere
    near midnight).
    """
    def test_gen_latest_rib_fn(self):
        """
        For the parent function test that any string is returned for all known
        MRT archive types, and that an unknown archive type raises an exception
        """
        self.assertEqual(
            self.mrt_as57355.gen_latest_rib_fn(),
            self.mrt_as57355.gen_latest_rib_fn_as57355()
        )

        self.assertEqual(
            self.mrt_ripe.gen_latest_rib_fn(),
            self.mrt_ripe.gen_latest_rib_fn_ripe()
        )

        self.assertEqual(
            self.mrt_rv.gen_latest_rib_fn(),
            self.mrt_rv.gen_latest_rib_fn_rv()
        )

        self.mrt_rv.TYPE = "HcSHqWb3C9i2jZqnzVj1"
        self.assertRaises(ValueError, self.mrt_rv.gen_latest_rib_fn)
        self.mrt_rv.TYPE = self.RV_TYPE

    def test_gen_latest_rib_fn_as57355(self):
        rib_name = self.mrt_as57355.gen_latest_rib_fn_as57355()
        regex = (
            f"{self.mrt_as57355.RIB_PREFIX}"
            f"{datetime.datetime.strftime(datetime.datetime.now(), '%Y')}"
            f"({datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(days=1), '%m%d')}|"
            f"{datetime.datetime.strftime(datetime.datetime.now(), '%m%d')})"
            f"\.([0-1][0-9]|2[0-3])[0-5][0-9]"
            f"\.{self.mrt_as57355.MRT_EXT}"
        )
        self.assertTrue(re.match(regex, rib_name))
        mins = int(rib_name.split(".")[2][:2]) * 60 + int(rib_name.split(".")[2][2:])
        self.assertTrue(mins % self.mrt_as57355.RIB_INTERVAL == 0)

    def test_gen_latest_rib_fn_ripe(self):
        rib_name = self.mrt_ripe.gen_latest_rib_fn_ripe()
        regex = (
            f"{self.mrt_ripe.RIB_PREFIX}"
            f"{datetime.datetime.strftime(datetime.datetime.now(), '%Y')}"
            f"({datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(days=1), '%m%d')}|"
            f"{datetime.datetime.strftime(datetime.datetime.now(), '%m%d')})"
            f"\.([0-1][0-9]|2[0-3])[0-5][0-9]"
            f"\.{self.mrt_ripe.MRT_EXT}"
        )
        self.assertTrue(re.match(regex, rib_name))
        mins = (int(rib_name.split(".")[2][:2]) * 60) + int(rib_name.split(".")[2][2:])
        self.assertTrue(mins % self.mrt_ripe.RIB_INTERVAL == 0)

    def test_gen_latest_rib_fn_rv(self):
        rib_name = self.mrt_rv.gen_latest_rib_fn_rv()
        regex = (
            f"{self.mrt_rv.RIB_PREFIX}"
            f"{datetime.datetime.strftime(datetime.datetime.now(), '%Y')}"
            f"({datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(days=1), '%m%d')}|"
            f"{datetime.datetime.strftime(datetime.datetime.now(), '%m%d')})"
            f"\.([0-1][0-9]|2[0-3])[0-5][0-9]"
            f"\.{self.mrt_rv.MRT_EXT}"
        )
        self.assertTrue(re.match(regex, rib_name))
        mins = int(rib_name.split(".")[2][:2]) * 60 + int(rib_name.split(".")[2][2:])
        self.assertTrue(mins % self.mrt_rv.RIB_INTERVAL == 0)

    def test_gen_latest_upd_fn(self):
        """
        For the parent function test that any string is returned for all known
        MRT archive types, and that an unknown archive type raises an exception
        """
        self.assertEqual(
            self.mrt_as57355.gen_latest_upd_fn(),
            self.mrt_as57355.gen_latest_upd_fn_as57355()
        )

        self.assertEqual(
            self.mrt_ripe.gen_latest_upd_fn(),
            self.mrt_ripe.gen_latest_upd_fn_ripe()
        )

        self.assertEqual(
            self.mrt_rv.gen_latest_upd_fn(),
            self.mrt_rv.gen_latest_upd_fn_rv()
        )

        self.mrt_rv.TYPE = "HcSHqWb3C9i2jZqnzVj1"
        self.assertRaises(ValueError, self.mrt_rv.gen_latest_upd_fn)
        self.mrt_rv.TYPE = self.RV_TYPE

    def test_gen_latest_upd_fn_as57355(self):
        upd_name = self.mrt_as57355.gen_latest_upd_fn_as57355()
        regex = (
            f"{self.mrt_as57355.UPD_PREFIX}"
            f"{datetime.datetime.strftime(datetime.datetime.now(), '%Y')}"
            f"({datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(days=1), '%m%d')}|"
            f"{datetime.datetime.strftime(datetime.datetime.now(), '%m%d')})"
            f"\.([0-1][0-9]|2[0-3])[0-5][0-9]"
            f"\.{self.mrt_as57355.MRT_EXT}"
        )
        self.assertTrue(re.match(regex, upd_name))
        mins = int(upd_name.split(".")[1][:2]) * 60 + int(upd_name.split(".")[1][2:])
        self.assertTrue(mins % self.mrt_as57355.UPD_INTERVAL == 0)

    def test_gen_latest_upd_fn_ripe(self):
        upd_name = self.mrt_ripe.gen_latest_upd_fn_ripe()
        regex = (
            f"{self.mrt_ripe.UPD_PREFIX}"
            f"{datetime.datetime.strftime(datetime.datetime.now(), '%Y')}"
            f"({datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(days=1), '%m%d')}|"
            f"{datetime.datetime.strftime(datetime.datetime.now(), '%m%d')})"
            f"\.([0-1][0-9]|2[0-3])[0-5][0-9]"
            f"\.{self.mrt_ripe.MRT_EXT}"
        )
        self.assertTrue(re.match(regex, upd_name))
        mins = int(upd_name.split(".")[2][:2]) * 60 + int(upd_name.split(".")[2][2:])
        self.assertTrue(mins % self.mrt_ripe.UPD_INTERVAL == 0)

    def test_gen_latest_upd_fn_rv(self):
        upd_name = self.mrt_rv.gen_latest_upd_fn_rv()
        regex = (
            f"{self.mrt_rv.UPD_PREFIX}"
            f"{datetime.datetime.strftime(datetime.datetime.now(), '%Y')}"
            f"({datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(days=1), '%m%d')}|"
            f"{datetime.datetime.strftime(datetime.datetime.now(), '%m%d')})"
            f"\.([0-1][0-9]|2[0-3])[0-5][0-9]"
            f"\.{self.mrt_rv.MRT_EXT}"
        )
        self.assertTrue(re.match(regex, upd_name))
        mins = int(upd_name.split(".")[2][:2]) * 60 + int(upd_name.split(".")[2][2:])
        self.assertTrue(mins % self.mrt_rv.UPD_INTERVAL == 0)

    def test_gen_rib_fn_date(self):
        self.assertEqual(
            self.mrt_as57355.gen_rib_fn_date("20220101.0000"),
            "rib.20220101.0000.dump"
        )
        self.assertEqual(
            self.mrt_ripe.gen_rib_fn_date("20220101.0000"),
            "bview.20220101.0000.gz"
        )
        self.assertEqual(
            self.mrt_rv.gen_rib_fn_date("20220101.0000"),
            "rib.20220101.0000.bz2"
        )

    def test_gen_rib_fns_day(self):
        as57355_20220101 = [
            'rib.20220101.0000.dump', 'rib.20220101.0100.dump',
            'rib.20220101.0200.dump', 'rib.20220101.0300.dump',
            'rib.20220101.0400.dump', 'rib.20220101.0500.dump',
            'rib.20220101.0600.dump', 'rib.20220101.0700.dump',
            'rib.20220101.0800.dump', 'rib.20220101.0900.dump',
            'rib.20220101.1000.dump', 'rib.20220101.1100.dump',
            'rib.20220101.1200.dump', 'rib.20220101.1300.dump',
            'rib.20220101.1400.dump', 'rib.20220101.1500.dump',
            'rib.20220101.1600.dump', 'rib.20220101.1700.dump',
            'rib.20220101.1800.dump', 'rib.20220101.1900.dump',
            'rib.20220101.2000.dump', 'rib.20220101.2100.dump',
            'rib.20220101.2200.dump', 'rib.20220101.2300.dump'
        ]
        ripe_20220101 = [
            'bview.20220101.0000.gz', 'bview.20220101.0800.gz',
            'bview.20220101.1600.gz'
        ]
        rv_20220101 = [
            'rib.20220101.0000.bz2', 'rib.20220101.0200.bz2',
            'rib.20220101.0400.bz2', 'rib.20220101.0600.bz2',
            'rib.20220101.0800.bz2', 'rib.20220101.1000.bz2',
            'rib.20220101.1200.bz2', 'rib.20220101.1400.bz2',
            'rib.20220101.1600.bz2', 'rib.20220101.1800.bz2',
            'rib.20220101.2000.bz2', 'rib.20220101.2200.bz2'
        ]
        self.assertEqual(
            self.mrt_as57355.gen_rib_fns_day("20220101"),
            as57355_20220101
        )
        self.assertEqual(
            self.mrt_ripe.gen_rib_fns_day("20220101"),
            ripe_20220101
        )
        self.assertEqual(
            self.mrt_rv.gen_rib_fns_day("20220101"),
            rv_20220101
        )

    def test_gen_rib_fns_range(self):
        as57355_20220101 = [
            '20220101.2300.dump', '20220102.0000.dump',
            '20220102.0100.dump'
        ]
        ripe_20220101 = [
            'updates.20220102.0000.gz', 'updates.20220102.0800.gz'
        ]
        rv_20220101 = [
            'updates.20220102.0000.bz2', 'updates.20220102.0200.bz2',
            'updates.20220102.0400.bz2'
        ]
        self.assertEqual(
            self.mrt_as57355.gen_rib_fns_range(
                start_date="20220101.2300", end_date="20220102.0100"
            ),
            as57355_20220101
        )
        self.assertEqual(
            self.mrt_ripe.gen_rib_fns_range(
                start_date="20220101.2300", end_date="20220102.0900"
            ),
            ripe_20220101
        )
        self.assertEqual(
            self.mrt_rv.gen_rib_fns_range(
                start_date="20220101.2300", end_date="20220102.0500"
            ),
            rv_20220101
        )

    def test_gen_rib_key(self):
        self.assertEqual(
            self.mrt_as57355.gen_rib_key("20220101"),
            "UNIT_TEST_AS57355_RIB:20220101"
        )
        self.assertEqual(
            self.mrt_ripe.gen_rib_key("20220101"),
            "UNIT_TEST_RIPE_RIB:20220101"
        )
        self.assertEqual(
            self.mrt_rv.gen_rib_key("20220101"),
            "UNIT_TEST_RV_RIB:20220101"
        )

    def test_gen_rib_url(self):
        as57355_url = "http://192.168.58.8:8000/lukasz/rib/rib.20220101.0000.dump"
        ripe_url = (
            "https://data.ris.ripe.net/rrc23/2022.01/bview.20220101.0000.gz"
        )
        rv_url = (
            "http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/"
            "RIBS/rib.20220101.0000.bz2"
        )
        self.assertEqual(
            self.mrt_as57355.gen_rib_url("rib.20220101.0000.dump"), as57355_url
        )
        self.assertEqual(
            self.mrt_ripe.gen_rib_url("bview.20220101.0000.gz"), ripe_url
        )
        self.assertEqual(
            self.mrt_rv.gen_rib_url("rib.20220101.0000.bz2"), rv_url
        )

if __name__ == '__main__':
    unittest.main()
