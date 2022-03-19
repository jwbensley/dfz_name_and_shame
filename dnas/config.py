import os

class config:
    """
    Class to store global config options.
    """

    def __init__(self):

        #################
        # BASE SETTINGS #
        #################

        # Application root for everything (logging, downloads, tmp files, etc.)
        self.BASE_DIR = "/media/usb0/"

        # Log mode, 'a'ppend or over'w'rite
        self.LOG_MODE = "a"
        # Log directory for all logs
        self.LOG_DIR = os.path.join(self.BASE_DIR, "logs/")
        # Logging from script: get_mrts.py
        self.LOG_GETTER = os.path.join(self.LOG_DIR, "get_mrts.log")
        # Logging from script: global_stats.py
        self.LOG_STATS = os.path.join(self.LOG_DIR, "global_stats.log")
        # Logging from script: parse_mrts.py
        self.LOG_PARSER = os.path.join(self.LOG_DIR, "parse_mrts.log")
        # Logging from script: gen_tweets.py
        self.LOG_TWITTER = os.path.join(self.LOG_DIR, "gen_tweets.log")


        """
        The time format used for generating new timestamps and parsing existing
        timestamps. The format below is the same format used by the MRT archives.
        """
        self.TIME_FORMAT = "%Y%m%d.%H%M"


        ####################
        # TWITTER SETTINGS #
        ####################

        # Twitter username
        self.TWITTER_USER = "bgp_shamer"
        self.TWITTER_LEN = 280


        ########################
        # MRT ARCHIVE SETTINGS #
        ########################

        # Base dir to save MRT files to
        self.DL_DIR = os.path.join(self.BASE_DIR, "downloads/")

        # Temporary directory to split MRT files into
        self.SPLIT_DIR = "/tmp/" # Set to None to disable

        """
        If the machine running this code is in a different timezone to the
        route-views MRT archives, an additional offset in hous is required.
        A negative int is "hours in the future", a positive int is
        "hours in the past".
        """
        self.RV_RIB_OFFSET = 0
        self.RV_UPD_OFFSET = 1
        self.RCC_RIB_OFFSET = 0
        self.RCC_UPD_OFFSET = 1


        # Locations of popular MRT dump archives:
        self.MRT_ARCHIVES = []

        """
        Route-Views London
        RIB dumps are every 2 hours
        RIB dump example: http://archive.routeviews.org/route-views.linx/bgpdata/2021.12/RIBS/rib.20211222.0600.bz2
        UPDATE dumps are every 15 minutes
        UPDATE dump example: http://archive.routeviews.org/route-views.linx/bgpdata/2021.12/UPDATES/updates.20211222.0600.bz2
        """
        self.MRT_ARCHIVES.append(
            {
                "TYPE": "RV",
                "NAME": "RV_LINX",
                "ENABLED": True,
                "BASE_URL": "http://archive.routeviews.org/route-views.linx/bgpdata/",
                "RIB_URL": "/RIBS/",
                "UPD_URL": "/UPDATES/",
                "MRT_EXT": "bz2",
                "MRT_DIR": os.path.join(self.DL_DIR, "LINX/"),
                "RIB_GLOB": "rib.*bz2",
                "UPD_GLOB": "updates.*bz2",
                "RIB_KEY": "RV_LINX_RIB",
                "UPD_KEY": "RV_LINX_UPD",
                "RIB_INTERVAL": 120,
                "UPD_INTERVAL": 15,
                "RIB_PREFIX": "rib.",
                "UPD_PREFIX": "updates.",
            }
        )

        """
        Route-Views Sydnex
        RIB dumps are every 2 hours
        RIB dump example: http://archive.routeviews.org/route-views.sydney/bgpdata/2021.12/RIBS/rib.20211201.0600.bz2
        UPDATE dumps are every 15 minutes
        UPDATE dump example: http://archive.routeviews.org/route-views.sydney/bgpdata/2021.12/UPDATES/updates.20211201.0030.bz2
        """
        self.MRT_ARCHIVES.append(
            {
                "TYPE": "RV",
                "NAME": "RV_SYDNEY",
                "ENABLED": False,
                "BASE_URL": "http://archive.routeviews.org/route-views.sydney/bgpdata/",
                "RIB_URL": "/RIBS/",
                "UPD_URL": "/UPDATES/",
                "MRT_EXT": "bz2",
                "MRT_DIR": os.path.join(self.DL_DIR, "SYDNEY/"),
                "RIB_GLOB": "rib.*bz2",
                "UPD_GLOB": "updates.*bz2",
                "RIB_KEY": "RV_SYDNEY_RIB",
                "UPD_KEY": "RV_SYDNEY_UPD",
                "RIB_INTERVAL": 120,
                "UPD_INTERVAL": 15,
                "RIB_PREFIX": "rib.",
                "UPD_PREFIX": "updates.",
            }
        )

        """
        RRC23 Singapore
        RIB dumps are every 8 hours
        RIB dump example: https://data.ris.ripe.net/rrc23/2021.12/bview.20211206.1600.gz
        UPDATE dumps are every 5 minutes
        UPDATE dump example: https://data.ris.ripe.net/rrc23/2021.12/updates.20211231.2335.gz
        """
        self.MRT_ARCHIVES.append(
            {
                "TYPE": "RIPE",
                "NAME": "RCC_23",
                "ENABLED": False,
                "BASE_URL": "https://data.ris.ripe.net/rrc23/",
                "RIB_URL": "/",
                "UPD_URL": "/",
                "MRT_EXT": "gz",
                "MRT_DIR": os.path.join(self.DL_DIR, "RCC23/"),
                "RIB_GLOB": "bview.*gz",
                "UPD_GLOB": "updates.*gz",
                "RIB_KEY": "RCC_23_RIB",
                "UPD_KEY": "RCC_23_UPD",
                "RIB_INTERVAL": 480,
                "UPD_INTERVAL": 5,
                "RIB_PREFIX": "bview.",
                "UPD_PREFIX": "updates.",
            }
        )

        """
        RRC24 LACNIC
        RIB dumps are every 8 hours
        RIB dump example: https://data.ris.ripe.net/rrc24/2021.12/bview.20211208.1600.gz
        UPDATE dumps are every 5 minutes
        UPDATE dump example: https://data.ris.ripe.net/rrc24/2021.12/updates.20211231.1245.gz
        """
        self.MRT_ARCHIVES.append(
            {
                "TYPE": "RIPE",
                "NAME": "RCC_24",
                "ENABLED": False,
                "BASE_URL": "https://data.ris.ripe.net/rrc24/",
                "RIB_URL": "/",
                "UPD_URL": "/",
                "MRT_EXT": "gz",
                "MRT_DIR": os.path.join(self.DL_DIR, "RCC24/"),
                "RIB_GLOB": "bview.*gz",
                "UPD_GLOB": "updates.*gz",
                "RIB_KEY": "RCC_24_RIB",
                "UPD_KEY": "RCC_24_UPD",
                "RIB_INTERVAL": 480,
                "UPD_INTERVAL": 5,
                "RIB_PREFIX": "bview.",
                "UPD_PREFIX": "updates.",
            }
        )
