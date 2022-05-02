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
        Check that the wrapper function calls the correct child function,
        and that an error is thrown if there is no matching child function:
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
        Check that the wrapper function calls the correct child function,
        and that an error is thrown if there is no matching child function:
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
            'rib.20220101.2300.dump', 'rib.20220102.0000.dump',
            'rib.20220102.0100.dump'
        ]
        ripe_20220101 = [
            'bview.20220102.0000.gz', 'bview.20220102.0800.gz'
        ]
        rv_20220101 = [
            'rib.20220102.0000.bz2', 'rib.20220102.0200.bz2',
            'rib.20220102.0400.bz2'
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

        self.assertRaises(ValueError, self.mrt_rv.gen_rib_key, "")
        self.assertRaises(TypeError, self.mrt_rv.gen_rib_key, 123)

    def test_gen_rib_url(self):
        """
        Check that the wrapper function calls the correct child function,
        and that an error is thrown if there is no matching child function:
        """
        self.assertEqual(
            self.mrt_as57355.gen_rib_url("rib.20220101.0000.dump"),
            self.mrt_as57355.gen_rib_url_as57355("rib.20220101.0000.dump")
        )

        self.assertEqual(
            self.mrt_ripe.gen_rib_url("bview.20220101.0000.gz"),
            self.mrt_ripe.gen_rib_url_ripe("bview.20220101.0000.gz")
        )

        self.assertEqual(
            self.mrt_rv.gen_rib_url("rib.20220101.0000.bz2"),
            self.mrt_rv.gen_rib_url_rv("rib.20220101.0000.bz2")
        )

        self.mrt_rv.TYPE = "HcSHqWb3C9i2jZqnzVj1"
        self.assertRaises(ValueError, self.mrt_rv.gen_rib_url)
        self.mrt_rv.TYPE = self.RV_TYPE

    def test_gen_rib_url_as57355(self):
        as57355_url = "http://192.168.58.8:8000/lukasz/rib/rib.20220101.0000.dump"
        self.assertEqual(
            self.mrt_as57355.gen_rib_url_as57355("rib.20220101.0000.dump"), as57355_url
        )

    def test_gen_rib_url_ripe(self):
        ripe_url = (
            "https://data.ris.ripe.net/rrc23/2022.01/bview.20220101.0000.gz"
        )
        self.assertEqual(
            self.mrt_ripe.gen_rib_url_ripe("bview.20220101.0000.gz"), ripe_url
        )

    def test_gen_rib_url_rv(self):
        rv_url = (
            "http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/"
            "RIBS/rib.20220101.0000.bz2"
        )
        self.assertEqual(
            self.mrt_rv.gen_rib_url_rv("rib.20220101.0000.bz2"), rv_url
        )

    def test_gen_rib_url_range(self):
        as57355_urls = [
            'http://192.168.58.8:8000/lukasz/rib/rib.20220101.2300.dump',
            'http://192.168.58.8:8000/lukasz/rib/rib.20220102.0000.dump',
            'http://192.168.58.8:8000/lukasz/rib/rib.20220102.0100.dump'
        ]
        self.assertEqual(
            self.mrt_as57355.gen_rib_url_range(
                end_date="20220102.0100", start_date="20220101.2300"
            ),
            as57355_urls
        )
        ripe_urls = [
            'https://data.ris.ripe.net/rrc23/2022.01/bview.20220102.0000.gz'
        ]
        self.assertEqual(
            self.mrt_ripe.gen_rib_url_range(
                end_date="20220102.0100", start_date="20220101.2300"
            ),
            ripe_urls
        )
        rv_urls = [
            ("http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/"
            "RIBS/rib.20220102.0000.bz2")
        ]
        self.assertEqual(
            self.mrt_rv.gen_rib_url_range(
                end_date="20220102.0100", start_date="20220101.2300"
            ),
            rv_urls
        )

    def test_gen_upd_fn_date(self):
        self.assertEqual(
            self.mrt_as57355.gen_upd_fn_date("20220101.0000"),
            "20220101.0000.dump"
        )
        self.assertEqual(
            self.mrt_ripe.gen_upd_fn_date("20220101.0000"),
            "updates.20220101.0000.gz"
        )
        self.assertEqual(
            self.mrt_rv.gen_upd_fn_date("20220101.0000"),
            "updates.20220101.0000.bz2"
        )

        self.assertRaises(ValueError, self.mrt_rv.gen_upd_fn_date, "")
        self.assertRaises(TypeError, self.mrt_rv.gen_upd_fn_date, 123)

    def test_gen_upd_fns_day(self):
        as57355_20220101 = [
            '20220101.0000.dump', '20220101.0010.dump', '20220101.0020.dump',
            '20220101.0030.dump', '20220101.0040.dump', '20220101.0050.dump',
            '20220101.0100.dump', '20220101.0110.dump', '20220101.0120.dump',
            '20220101.0130.dump', '20220101.0140.dump', '20220101.0150.dump',
            '20220101.0200.dump', '20220101.0210.dump', '20220101.0220.dump',
            '20220101.0230.dump', '20220101.0240.dump', '20220101.0250.dump',
            '20220101.0300.dump', '20220101.0310.dump', '20220101.0320.dump',
            '20220101.0330.dump', '20220101.0340.dump', '20220101.0350.dump',
            '20220101.0400.dump', '20220101.0410.dump', '20220101.0420.dump',
            '20220101.0430.dump', '20220101.0440.dump', '20220101.0450.dump',
            '20220101.0500.dump', '20220101.0510.dump', '20220101.0520.dump',
            '20220101.0530.dump', '20220101.0540.dump', '20220101.0550.dump',
            '20220101.0600.dump', '20220101.0610.dump', '20220101.0620.dump',
            '20220101.0630.dump', '20220101.0640.dump', '20220101.0650.dump',
            '20220101.0700.dump', '20220101.0710.dump', '20220101.0720.dump',
            '20220101.0730.dump', '20220101.0740.dump', '20220101.0750.dump',
            '20220101.0800.dump', '20220101.0810.dump', '20220101.0820.dump',
            '20220101.0830.dump', '20220101.0840.dump', '20220101.0850.dump',
            '20220101.0900.dump', '20220101.0910.dump', '20220101.0920.dump',
            '20220101.0930.dump', '20220101.0940.dump', '20220101.0950.dump',
            '20220101.1000.dump', '20220101.1010.dump', '20220101.1020.dump',
            '20220101.1030.dump', '20220101.1040.dump', '20220101.1050.dump',
            '20220101.1100.dump', '20220101.1110.dump', '20220101.1120.dump',
            '20220101.1130.dump', '20220101.1140.dump', '20220101.1150.dump',
            '20220101.1200.dump', '20220101.1210.dump', '20220101.1220.dump',
            '20220101.1230.dump', '20220101.1240.dump', '20220101.1250.dump',
            '20220101.1300.dump', '20220101.1310.dump', '20220101.1320.dump',
            '20220101.1330.dump', '20220101.1340.dump', '20220101.1350.dump',
            '20220101.1400.dump', '20220101.1410.dump', '20220101.1420.dump',
            '20220101.1430.dump', '20220101.1440.dump', '20220101.1450.dump', 
            '20220101.1500.dump', '20220101.1510.dump', '20220101.1520.dump',
            '20220101.1530.dump', '20220101.1540.dump', '20220101.1550.dump',
            '20220101.1600.dump', '20220101.1610.dump', '20220101.1620.dump',
            '20220101.1630.dump', '20220101.1640.dump', '20220101.1650.dump',
            '20220101.1700.dump', '20220101.1710.dump', '20220101.1720.dump',
            '20220101.1730.dump', '20220101.1740.dump', '20220101.1750.dump',
            '20220101.1800.dump', '20220101.1810.dump', '20220101.1820.dump',
            '20220101.1830.dump', '20220101.1840.dump', '20220101.1850.dump',
            '20220101.1900.dump', '20220101.1910.dump', '20220101.1920.dump',
            '20220101.1930.dump', '20220101.1940.dump', '20220101.1950.dump',
            '20220101.2000.dump', '20220101.2010.dump', '20220101.2020.dump',
            '20220101.2030.dump', '20220101.2040.dump', '20220101.2050.dump',
            '20220101.2100.dump', '20220101.2110.dump', '20220101.2120.dump',
            '20220101.2130.dump', '20220101.2140.dump', '20220101.2150.dump',
            '20220101.2200.dump', '20220101.2210.dump', '20220101.2220.dump',
            '20220101.2230.dump', '20220101.2240.dump', '20220101.2250.dump',
            '20220101.2300.dump', '20220101.2310.dump', '20220101.2320.dump',
            '20220101.2330.dump', '20220101.2340.dump', '20220101.2350.dump'
        ]
        ripe_20220101 = [
            'updates.20220101.0000.gz', 'updates.20220101.0005.gz',
            'updates.20220101.0010.gz', 'updates.20220101.0015.gz',
            'updates.20220101.0020.gz', 'updates.20220101.0025.gz',
            'updates.20220101.0030.gz', 'updates.20220101.0035.gz',
            'updates.20220101.0040.gz', 'updates.20220101.0045.gz',
            'updates.20220101.0050.gz', 'updates.20220101.0055.gz',
            'updates.20220101.0100.gz', 'updates.20220101.0105.gz',
            'updates.20220101.0110.gz', 'updates.20220101.0115.gz',
            'updates.20220101.0120.gz', 'updates.20220101.0125.gz',
            'updates.20220101.0130.gz', 'updates.20220101.0135.gz',
            'updates.20220101.0140.gz', 'updates.20220101.0145.gz',
            'updates.20220101.0150.gz', 'updates.20220101.0155.gz',
            'updates.20220101.0200.gz', 'updates.20220101.0205.gz',
            'updates.20220101.0210.gz', 'updates.20220101.0215.gz',
            'updates.20220101.0220.gz', 'updates.20220101.0225.gz',
            'updates.20220101.0230.gz', 'updates.20220101.0235.gz',
            'updates.20220101.0240.gz', 'updates.20220101.0245.gz',
            'updates.20220101.0250.gz', 'updates.20220101.0255.gz',
            'updates.20220101.0300.gz', 'updates.20220101.0305.gz',
            'updates.20220101.0310.gz', 'updates.20220101.0315.gz',
            'updates.20220101.0320.gz', 'updates.20220101.0325.gz',
            'updates.20220101.0330.gz', 'updates.20220101.0335.gz',
            'updates.20220101.0340.gz', 'updates.20220101.0345.gz',
            'updates.20220101.0350.gz', 'updates.20220101.0355.gz',
            'updates.20220101.0400.gz', 'updates.20220101.0405.gz',
            'updates.20220101.0410.gz', 'updates.20220101.0415.gz',
            'updates.20220101.0420.gz', 'updates.20220101.0425.gz',
            'updates.20220101.0430.gz', 'updates.20220101.0435.gz',
            'updates.20220101.0440.gz', 'updates.20220101.0445.gz',
            'updates.20220101.0450.gz', 'updates.20220101.0455.gz',
            'updates.20220101.0500.gz', 'updates.20220101.0505.gz',
            'updates.20220101.0510.gz', 'updates.20220101.0515.gz',
            'updates.20220101.0520.gz', 'updates.20220101.0525.gz',
            'updates.20220101.0530.gz', 'updates.20220101.0535.gz',
            'updates.20220101.0540.gz', 'updates.20220101.0545.gz',
            'updates.20220101.0550.gz', 'updates.20220101.0555.gz',
            'updates.20220101.0600.gz', 'updates.20220101.0605.gz',
            'updates.20220101.0610.gz', 'updates.20220101.0615.gz',
            'updates.20220101.0620.gz', 'updates.20220101.0625.gz',
            'updates.20220101.0630.gz', 'updates.20220101.0635.gz',
            'updates.20220101.0640.gz', 'updates.20220101.0645.gz',
            'updates.20220101.0650.gz', 'updates.20220101.0655.gz',
            'updates.20220101.0700.gz', 'updates.20220101.0705.gz',
            'updates.20220101.0710.gz', 'updates.20220101.0715.gz',
            'updates.20220101.0720.gz', 'updates.20220101.0725.gz',
            'updates.20220101.0730.gz', 'updates.20220101.0735.gz',
            'updates.20220101.0740.gz', 'updates.20220101.0745.gz',
            'updates.20220101.0750.gz', 'updates.20220101.0755.gz',
            'updates.20220101.0800.gz', 'updates.20220101.0805.gz',
            'updates.20220101.0810.gz', 'updates.20220101.0815.gz',
            'updates.20220101.0820.gz', 'updates.20220101.0825.gz',
            'updates.20220101.0830.gz', 'updates.20220101.0835.gz',
            'updates.20220101.0840.gz', 'updates.20220101.0845.gz',
            'updates.20220101.0850.gz', 'updates.20220101.0855.gz',
            'updates.20220101.0900.gz', 'updates.20220101.0905.gz',
            'updates.20220101.0910.gz', 'updates.20220101.0915.gz',
            'updates.20220101.0920.gz', 'updates.20220101.0925.gz',
            'updates.20220101.0930.gz', 'updates.20220101.0935.gz',
            'updates.20220101.0940.gz', 'updates.20220101.0945.gz',
            'updates.20220101.0950.gz', 'updates.20220101.0955.gz',
            'updates.20220101.1000.gz', 'updates.20220101.1005.gz',
            'updates.20220101.1010.gz', 'updates.20220101.1015.gz',
            'updates.20220101.1020.gz', 'updates.20220101.1025.gz',
            'updates.20220101.1030.gz', 'updates.20220101.1035.gz',
            'updates.20220101.1040.gz', 'updates.20220101.1045.gz',
            'updates.20220101.1050.gz', 'updates.20220101.1055.gz',
            'updates.20220101.1100.gz', 'updates.20220101.1105.gz',
            'updates.20220101.1110.gz', 'updates.20220101.1115.gz',
            'updates.20220101.1120.gz', 'updates.20220101.1125.gz',
            'updates.20220101.1130.gz', 'updates.20220101.1135.gz',
            'updates.20220101.1140.gz', 'updates.20220101.1145.gz',
            'updates.20220101.1150.gz', 'updates.20220101.1155.gz',
            'updates.20220101.1200.gz', 'updates.20220101.1205.gz',
            'updates.20220101.1210.gz', 'updates.20220101.1215.gz',
            'updates.20220101.1220.gz', 'updates.20220101.1225.gz',
            'updates.20220101.1230.gz', 'updates.20220101.1235.gz',
            'updates.20220101.1240.gz', 'updates.20220101.1245.gz',
            'updates.20220101.1250.gz', 'updates.20220101.1255.gz',
            'updates.20220101.1300.gz', 'updates.20220101.1305.gz',
            'updates.20220101.1310.gz', 'updates.20220101.1315.gz',
            'updates.20220101.1320.gz', 'updates.20220101.1325.gz',
            'updates.20220101.1330.gz', 'updates.20220101.1335.gz',
            'updates.20220101.1340.gz', 'updates.20220101.1345.gz',
            'updates.20220101.1350.gz', 'updates.20220101.1355.gz',
            'updates.20220101.1400.gz', 'updates.20220101.1405.gz',
            'updates.20220101.1410.gz', 'updates.20220101.1415.gz',
            'updates.20220101.1420.gz', 'updates.20220101.1425.gz',
            'updates.20220101.1430.gz', 'updates.20220101.1435.gz',
            'updates.20220101.1440.gz', 'updates.20220101.1445.gz',
            'updates.20220101.1450.gz', 'updates.20220101.1455.gz',
            'updates.20220101.1500.gz', 'updates.20220101.1505.gz',
            'updates.20220101.1510.gz', 'updates.20220101.1515.gz',
            'updates.20220101.1520.gz', 'updates.20220101.1525.gz',
            'updates.20220101.1530.gz', 'updates.20220101.1535.gz',
            'updates.20220101.1540.gz', 'updates.20220101.1545.gz',
            'updates.20220101.1550.gz', 'updates.20220101.1555.gz',
            'updates.20220101.1600.gz', 'updates.20220101.1605.gz',
            'updates.20220101.1610.gz', 'updates.20220101.1615.gz',
            'updates.20220101.1620.gz', 'updates.20220101.1625.gz',
            'updates.20220101.1630.gz', 'updates.20220101.1635.gz',
            'updates.20220101.1640.gz', 'updates.20220101.1645.gz',
            'updates.20220101.1650.gz', 'updates.20220101.1655.gz',
            'updates.20220101.1700.gz', 'updates.20220101.1705.gz',
            'updates.20220101.1710.gz', 'updates.20220101.1715.gz',
            'updates.20220101.1720.gz', 'updates.20220101.1725.gz',
            'updates.20220101.1730.gz', 'updates.20220101.1735.gz',
            'updates.20220101.1740.gz', 'updates.20220101.1745.gz',
            'updates.20220101.1750.gz', 'updates.20220101.1755.gz',
            'updates.20220101.1800.gz', 'updates.20220101.1805.gz',
            'updates.20220101.1810.gz', 'updates.20220101.1815.gz',
            'updates.20220101.1820.gz', 'updates.20220101.1825.gz',
            'updates.20220101.1830.gz', 'updates.20220101.1835.gz',
            'updates.20220101.1840.gz', 'updates.20220101.1845.gz',
            'updates.20220101.1850.gz', 'updates.20220101.1855.gz',
            'updates.20220101.1900.gz', 'updates.20220101.1905.gz',
            'updates.20220101.1910.gz', 'updates.20220101.1915.gz',
            'updates.20220101.1920.gz', 'updates.20220101.1925.gz',
            'updates.20220101.1930.gz', 'updates.20220101.1935.gz',
            'updates.20220101.1940.gz', 'updates.20220101.1945.gz',
            'updates.20220101.1950.gz', 'updates.20220101.1955.gz',
            'updates.20220101.2000.gz', 'updates.20220101.2005.gz',
            'updates.20220101.2010.gz', 'updates.20220101.2015.gz',
            'updates.20220101.2020.gz', 'updates.20220101.2025.gz',
            'updates.20220101.2030.gz', 'updates.20220101.2035.gz',
            'updates.20220101.2040.gz', 'updates.20220101.2045.gz',
            'updates.20220101.2050.gz', 'updates.20220101.2055.gz',
            'updates.20220101.2100.gz', 'updates.20220101.2105.gz',
            'updates.20220101.2110.gz', 'updates.20220101.2115.gz',
            'updates.20220101.2120.gz', 'updates.20220101.2125.gz',
            'updates.20220101.2130.gz', 'updates.20220101.2135.gz',
            'updates.20220101.2140.gz', 'updates.20220101.2145.gz',
            'updates.20220101.2150.gz', 'updates.20220101.2155.gz',
            'updates.20220101.2200.gz', 'updates.20220101.2205.gz',
            'updates.20220101.2210.gz', 'updates.20220101.2215.gz',
            'updates.20220101.2220.gz', 'updates.20220101.2225.gz',
            'updates.20220101.2230.gz', 'updates.20220101.2235.gz',
            'updates.20220101.2240.gz', 'updates.20220101.2245.gz',
            'updates.20220101.2250.gz', 'updates.20220101.2255.gz',
            'updates.20220101.2300.gz', 'updates.20220101.2305.gz',
            'updates.20220101.2310.gz', 'updates.20220101.2315.gz',
            'updates.20220101.2320.gz', 'updates.20220101.2325.gz',
            'updates.20220101.2330.gz', 'updates.20220101.2335.gz',
            'updates.20220101.2340.gz', 'updates.20220101.2345.gz',
            'updates.20220101.2350.gz', 'updates.20220101.2355.gz'
        ]
        rv_20220101 = [
            'updates.20220101.0000.bz2', 'updates.20220101.0015.bz2', 'updates.20220101.0030.bz2',
            'updates.20220101.0045.bz2', 'updates.20220101.0100.bz2', 'updates.20220101.0115.bz2',
            'updates.20220101.0130.bz2', 'updates.20220101.0145.bz2', 'updates.20220101.0200.bz2',
            'updates.20220101.0215.bz2', 'updates.20220101.0230.bz2', 'updates.20220101.0245.bz2',
            'updates.20220101.0300.bz2', 'updates.20220101.0315.bz2', 'updates.20220101.0330.bz2',
            'updates.20220101.0345.bz2', 'updates.20220101.0400.bz2', 'updates.20220101.0415.bz2',
            'updates.20220101.0430.bz2', 'updates.20220101.0445.bz2', 'updates.20220101.0500.bz2',
            'updates.20220101.0515.bz2', 'updates.20220101.0530.bz2', 'updates.20220101.0545.bz2',
            'updates.20220101.0600.bz2', 'updates.20220101.0615.bz2', 'updates.20220101.0630.bz2',
            'updates.20220101.0645.bz2', 'updates.20220101.0700.bz2', 'updates.20220101.0715.bz2',
            'updates.20220101.0730.bz2', 'updates.20220101.0745.bz2', 'updates.20220101.0800.bz2',
            'updates.20220101.0815.bz2', 'updates.20220101.0830.bz2', 'updates.20220101.0845.bz2',
            'updates.20220101.0900.bz2', 'updates.20220101.0915.bz2', 'updates.20220101.0930.bz2',
            'updates.20220101.0945.bz2', 'updates.20220101.1000.bz2', 'updates.20220101.1015.bz2',
            'updates.20220101.1030.bz2', 'updates.20220101.1045.bz2', 'updates.20220101.1100.bz2',
            'updates.20220101.1115.bz2', 'updates.20220101.1130.bz2', 'updates.20220101.1145.bz2',
            'updates.20220101.1200.bz2', 'updates.20220101.1215.bz2', 'updates.20220101.1230.bz2',
            'updates.20220101.1245.bz2', 'updates.20220101.1300.bz2', 'updates.20220101.1315.bz2',
            'updates.20220101.1330.bz2', 'updates.20220101.1345.bz2', 'updates.20220101.1400.bz2',
            'updates.20220101.1415.bz2', 'updates.20220101.1430.bz2', 'updates.20220101.1445.bz2',
            'updates.20220101.1500.bz2', 'updates.20220101.1515.bz2', 'updates.20220101.1530.bz2',
            'updates.20220101.1545.bz2', 'updates.20220101.1600.bz2', 'updates.20220101.1615.bz2',
            'updates.20220101.1630.bz2', 'updates.20220101.1645.bz2', 'updates.20220101.1700.bz2',
            'updates.20220101.1715.bz2', 'updates.20220101.1730.bz2', 'updates.20220101.1745.bz2',
            'updates.20220101.1800.bz2', 'updates.20220101.1815.bz2', 'updates.20220101.1830.bz2',
            'updates.20220101.1845.bz2', 'updates.20220101.1900.bz2', 'updates.20220101.1915.bz2',
            'updates.20220101.1930.bz2', 'updates.20220101.1945.bz2', 'updates.20220101.2000.bz2',
            'updates.20220101.2015.bz2', 'updates.20220101.2030.bz2', 'updates.20220101.2045.bz2',
            'updates.20220101.2100.bz2', 'updates.20220101.2115.bz2', 'updates.20220101.2130.bz2',
            'updates.20220101.2145.bz2', 'updates.20220101.2200.bz2', 'updates.20220101.2215.bz2',
            'updates.20220101.2230.bz2', 'updates.20220101.2245.bz2', 'updates.20220101.2300.bz2',
            'updates.20220101.2315.bz2', 'updates.20220101.2330.bz2', 'updates.20220101.2345.bz2'
        ]
        self.assertEqual(
            self.mrt_as57355.gen_upd_fns_day("20220101"),
            as57355_20220101
        )
        self.assertEqual(
            self.mrt_ripe.gen_upd_fns_day("20220101"),
            ripe_20220101
        )
        self.assertEqual(
            self.mrt_rv.gen_upd_fns_day("20220101"),
            rv_20220101
        )

    def test_gen_upd_fns_range(self):
        as57355_20220101 = [
            '20220101.2350.dump', '20220102.0000.dump',
            '20220102.0010.dump'
        ]
        ripe_20220101 = [
            'updates.20220101.2350.gz', 'updates.20220101.2355.gz',
            'updates.20220102.0000.gz', 'updates.20220102.0005.gz',
            'updates.20220102.0010.gz'
        ]
        rv_20220101 = ['updates.20220102.0000.bz2']
        self.assertEqual(
            self.mrt_as57355.gen_upd_fns_range(
                start_date="20220101.2350", end_date="20220102.0010"
            ),
            as57355_20220101
        )
        self.assertEqual(
            self.mrt_ripe.gen_upd_fns_range(
                start_date="20220101.2350", end_date="20220102.0010"
            ),
            ripe_20220101
        )
        self.assertEqual(
            self.mrt_rv.gen_upd_fns_range(
                start_date="20220101.2350", end_date="20220102.0010"
            ),
            rv_20220101
        )

    def test_gen_upd_key(self):
        self.assertEqual(
            self.mrt_as57355.gen_upd_key("20220101"),
            "UNIT_TEST_AS57355_UPD:20220101"
        )
        self.assertEqual(
            self.mrt_ripe.gen_upd_key("20220101"),
            "UNIT_TEST_RIPE_UPD:20220101"
        )
        self.assertEqual(
            self.mrt_rv.gen_upd_key("20220101"),
            "UNIT_TEST_RV_UPD:20220101"
        )

        self.assertRaises(ValueError, self.mrt_rv.gen_upd_key, "")
        self.assertRaises(TypeError, self.mrt_rv.gen_upd_key, 123)

    def test_gen_upd_url(self):
        """
        Check that the wrapper function calls the correct child function,
        and that an error is thrown if there is no matching child function:
        """
        self.assertEqual(
            self.mrt_as57355.gen_upd_url("20220101.0000.dump"),
            self.mrt_as57355.gen_upd_url_as57355("20220101.0000.dump")
        )

        self.assertEqual(
            self.mrt_ripe.gen_upd_url("updates.20220101.0000.gz"),
            self.mrt_ripe.gen_upd_url_ripe("updates.20220101.0000.gz")
        )

        self.assertEqual(
            self.mrt_rv.gen_upd_url("updates.20220101.0000.bz2"),
            self.mrt_rv.gen_upd_url_rv("updates.20220101.0000.bz2")
        )

        self.mrt_rv.TYPE = "HcSHqWb3C9i2jZqnzVj1"
        self.assertRaises(ValueError, self.mrt_rv.gen_upd_url)
        self.mrt_rv.TYPE = self.RV_TYPE

    def test_gen_upd_url_as57355(self):
        as57355_url = "http://192.168.58.8:8000/lukasz/update/20220101.0000.dump"
        self.assertEqual(
            self.mrt_as57355.gen_upd_url_as57355("20220101.0000.dump"), as57355_url
        )

    def test_gen_rib_url_ripe(self):
        ripe_url = (
            "https://data.ris.ripe.net/rrc23/2022.01/updates.20220101.0000.gz"
        )
        self.assertEqual(
            self.mrt_ripe.gen_upd_url_ripe("updates.20220101.0000.gz"), ripe_url
        )

    def test_gen_rib_url_rv(self):
        rv_url = (
            "http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/"
            "UPDATES/updates.20220101.0000.bz2"
        )
        self.assertEqual(
            self.mrt_rv.gen_upd_url_rv("updates.20220101.0000.bz2"), rv_url
        )

    def test_gen_upd_url_range(self):
        as57355_urls = [
            'http://192.168.58.8:8000/lukasz/update/20220101.2350.dump',
            'http://192.168.58.8:8000/lukasz/update/20220102.0000.dump',
            'http://192.168.58.8:8000/lukasz/update/20220102.0010.dump'
        ]
        self.assertEqual(
            self.mrt_as57355.gen_upd_url_range(
                end_date="20220102.0010", start_date="20220101.2350"
            ),
            as57355_urls
        )
        ripe_urls = [
            'https://data.ris.ripe.net/rrc23/2022.01/updates.20220101.2350.gz',
            'https://data.ris.ripe.net/rrc23/2022.01/updates.20220101.2355.gz',
            'https://data.ris.ripe.net/rrc23/2022.01/updates.20220102.0000.gz',
            'https://data.ris.ripe.net/rrc23/2022.01/updates.20220102.0005.gz',
            'https://data.ris.ripe.net/rrc23/2022.01/updates.20220102.0010.gz',
        ]
        self.assertEqual(
            self.mrt_ripe.gen_upd_url_range(
                end_date="20220102.0010", start_date="20220101.2350"
            ),
            ripe_urls
        )
        rv_urls = [
            ("http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/"
            "UPDATES/updates.20220101.2345.bz2"),
            ("http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/"
            "UPDATES/updates.20220102.0000.bz2"),
            ("http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/"
            "UPDATES/updates.20220102.0015.bz2")
        ]
        self.assertEqual(
            self.mrt_rv.gen_upd_url_range(
                end_date="20220102.0020", start_date="20220101.2345"
            ),
            rv_urls
        )

    def test_ts_from_filename(self):
        """
        Check that the wrapper function calls the correct child function,
        and that an error is thrown if there is no matching child function:
        """
        self.assertEqual(
            self.mrt_as57355.ts_from_filename(
                self.mrt_as57355.gen_upd_fn_date("20220101.0000")
            ),
            self.mrt_as57355.ts_from_filename_as57355(
                self.mrt_as57355.gen_upd_fn_date("20220101.0000")
            )
        )

        self.assertEqual(
            self.mrt_ripe.ts_from_filename(
                self.mrt_ripe.gen_upd_fn_date("20220101.0000")
            ),
            self.mrt_ripe.ts_from_filename_ripe(
                self.mrt_ripe.gen_upd_fn_date("20220101.0000")
            )
        )

        self.assertEqual(
            self.mrt_rv.ts_from_filename(
                self.mrt_rv.gen_upd_fn_date("20220101.0000")
            ),
            self.mrt_rv.ts_from_filename_rv(
                self.mrt_rv.gen_upd_fn_date("20220101.0000")
            )
        )

        self.mrt_rv.TYPE = "HcSHqWb3C9i2jZqnzVj1"
        self.assertRaises(
            ValueError,
            self.mrt_rv.ts_from_filename,
            self.mrt_rv.gen_upd_fn_date("20220101.0000")
        )
        self.mrt_rv.TYPE = self.RV_TYPE

    def test_ts_from_filename_as57355(self):
        self.assertEqual(
            self.mrt_as57355.ts_from_filename_as57355(
                self.mrt_as57355.gen_upd_fn_date("20220101.0000")
            ),
            datetime.datetime(2022, 1, 1, 0, 0)
        )

    def test_ts_from_filename_ripe(self):
        self.assertEqual(
            self.mrt_ripe.ts_from_filename_ripe(
                self.mrt_ripe.gen_upd_fn_date("20220101.0000")
            ),
            datetime.datetime(2022, 1, 1, 0, 0)
        )

    def test_ts_from_filename_rv(self):
        self.assertEqual(
            self.mrt_rv.ts_from_filename_rv(
                self.mrt_rv.gen_upd_fn_date("20220101.0000")
            ),
            datetime.datetime(2022, 1, 1, 0, 0)
        )

    def test_valid_ym(self):
        # Missing value
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ym, ""
        )
        # Not a string
        self.assertRaises(
            TypeError, self.mrt_rv.valid_ym, 202201
        )
        # Invalid year
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ym, "19701"
        )
        # Invalid month
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ym, "202213"
        )
        # Too long
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ym, "2022011"
        )
        # To short
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ym, "20221"
        )
        # Alphabetical chars
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ym, "20220a"
        )
        # Check nothing raised with valid value
        asserted = False
        try:
            self.mrt_rv.valid_ym("202201")
        except:
            asserted = True
        self.assertEqual(asserted, False)

    def test_valid_ymd(self):
        # Missing value
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd, ""
        )
        # Not a string
        self.assertRaises(
            TypeError, self.mrt_rv.valid_ymd, 20220101
        )
        # Invalid year
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd, "1970101"
        )
        # Invalid month
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd, "20221301"
        )
        # Invalid day
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd, "20220132"
        )
        # Too long
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd, "202201011"
        )
        # To short
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd, "2022010"
        )
        # Alphabetical chars
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd, "202201aa"
        )
        # Check nothing raised with valid value
        asserted = False
        try:
            self.mrt_rv.valid_ymd("20220101")
        except:
            asserted = True
        self.assertEqual(asserted, False)

    def test_valid_ymd_hm(self):
        # Missing value
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd_hm, ""
        )
        # Not a string
        self.assertRaises(
            TypeError, self.mrt_rv.valid_ymd_hm, 20220101
        )
        # Invalid year
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd_hm, "1970101.0000"
        )
        # Invalid month
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd_hm, "20221301.0000"
        )
        # Invalid day
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd_hm, "20220132.0000"
        )
        # Invalid hour
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd_hm, "20220101.2400"
        )
        # Invalid minute
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd_hm, "20220101.0060"
        )
        # Too long
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd_hm, "20220101.00000"
        )
        # To short
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd_hm, "20220101.000"
        )
        # Alphabetical chars
        self.assertRaises(
            ValueError, self.mrt_rv.valid_ymd_hm, "20220101.000a"
        )
        # Check nothing raised with valid value
        asserted = False
        try:
            self.mrt_rv.valid_ymd_hm("20220101.0000")
        except:
            asserted = True
        self.assertEqual(asserted, False)

    def test_ymd_from_file_path(self):
        self.assertEqual(
            self.mrt_as57355.ymd_from_file_path("20220101.0000.dump"),
            "20220101"
        )
        self.assertEqual(
            self.mrt_ripe.ymd_from_file_path("updates.20220101.0000.gz"),
            "20220101"
        )
        self.assertEqual(
            self.mrt_rv.ymd_from_file_path("updates.20220101.0000.bz2"),
            "20220101"
        )

        self.assertRaises(ValueError, self.mrt_rv.ymd_from_file_path, "")
        self.assertRaises(TypeError, self.mrt_rv.ymd_from_file_path, 123)
        self.assertRaises(
            ValueError,
            self.mrt_rv.ymd_from_file_path, "updates.20220101111.0000.bz2"
        )

        self.mrt_rv.TYPE = "HcSHqWb3C9i2jZqnzVj1"
        self.assertRaises(
            ValueError,
            self.mrt_rv.ymd_from_file_path,
            "updates.20220101.0000.bz2"
        )
        self.mrt_rv.TYPE = self.RV_TYPE

if __name__ == '__main__':
    unittest.main()
