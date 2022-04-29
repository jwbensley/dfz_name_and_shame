import os
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
    RV_RIB_OFFSET = 2
    RV_UPD_OFFSET = 2

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
    RIPE_RIB_OFFSET = 2
    RIPE_UPD_OFFSET = 2

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
    def test_gen_latest_rib_fn(self):
        #print(self.mrt_ripe.gen_latest_rib_fn())
        #print(self.mrt_rv.gen_latest_rib_fn())

    def test_gen_latest_upd_fn(self):
        #print(self.mrt_ripe.gen_latest_upd_fn())
        #print(self.mrt_rv.gen_latest_upd_fn())
    """

    def test_gen_rib_fn_date(self):
        self.assertEqual(self.mrt_ripe.gen_rib_fn_date("20220101.0000"), "bview.20220101.0000.gz")
        self.assertEqual(self.mrt_rv.gen_rib_fn_date("20220101.0000"), "rib.20220101.0000.bz2")

    def test_gen_rib_fns_day(self):
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
        self.assertEqual(self.mrt_ripe.gen_rib_fns_day("20220101"), ripe_20220101)
        self.assertEqual(self.mrt_rv.gen_rib_fns_day("20220101"), rv_20220101)

    """
    def test_gen_rib_fns_range(self):
        print(self.mrt_ripe.gen_rib_fns_range(start_date="20220101.2300", end_date="20220102.0100"))
        print(self.mrt_rv.gen_rib_fns_range(start_date="20220101.2300", end_date="20220102.0100"))
    """

    def test_gen_rib_key(self):
        self.assertEqual(self.mrt_ripe.gen_rib_key("20220101"), "UNIT_TEST_RIPE_RIB:20220101")
        self.assertEqual(self.mrt_rv.gen_rib_key("20220101"), "UNIT_TEST_RV_RIB:20220101")

    def test_gen_rib_url(self):
        ripe_url = "https://data.ris.ripe.net/rrc23/2022.01/bview.20220101.0000.gz"
        rv_url = "http://archive.routeviews.org/route-views.linx/bgpdata/2022.01/RIBS/rib.20220101.0000.bz2"
        self.assertEqual(self.mrt_ripe.gen_rib_url("bview.20220101.0000.gz"), ripe_url)
        self.assertEqual(self.mrt_rv.gen_rib_url("rib.20220101.0000.bz2"), rv_url)

if __name__ == '__main__':
    unittest.main()
