import os

class config:
    """
    Class to store global config options.
    """

    #################
    # BASE SETTINGS #
    #################

    # Application root for everything (logging, downloads, tmp files, etc.)
    BASE_DIR = "/media/usb0/"

    # Log mode, 'a'ppend or over'w'rite
    LOG_MODE = "a"
    # Standard logging format
    LOG_STANDARD = "%(asctime)s|%(levelname)s|%(message)s"
    # Debugging logging formart
    LOG_DEBUG = "%(asctime)s|%(levelname)s|%(process)d|%(funcName)s|%(message)s"
    # Log directory for all logs
    LOG_DIR = os.path.join(BASE_DIR, "logs/")
    # Logging from script: get_mrts.py
    LOG_GETTER = os.path.join(LOG_DIR, "get_mrts.log")
    # Logging from script: git_report.py
    LOG_GIT = os.path.join(LOG_DIR, "git_report.log")
    # Logging from script: parse_mrts.py
    LOG_PARSER = os.path.join(LOG_DIR, "parse_mrts.log")
    # Logging from script: redis_mgmt.py
    LOG_REDIS = os.path.join(LOG_DIR, "redis_mgmt.log")
    # Logging from script: split_mrt.py
    LOG_SPLITTER = os.path.join(LOG_DIR, "split_mrt.log")
    # Logging from script: stats.py
    LOG_STATS = os.path.join(LOG_DIR, "stats.log")
    # Logging from script: tweet.py
    LOG_TWITTER = os.path.join(LOG_DIR, "tweet.log")


    """
    The time format used for generating new timestamps and parsing existing
    timestamps. The format below is the same format used by the MRT archives.
    """
    TIME_FORMAT = "%Y%m%d.%H%M"
    DAY_FORMAT = "%Y%m%d"


    ################
    # GIT SETTINGS #
    ################

    # Remote git details
    GIT_STAT_BASE_URL = "https://github.com/DFZ-Name-and-Shame/dnas_stats/tree/main/"

    # Local git repo details
    GIT_BASE = os.path.join(BASE_DIR, "dnas_stats/")
    GIT_REPORT_BRANCH = "main"


    ####################
    # TWITTER SETTINGS #
    ####################

    # Twitter username
    TWITTER_USER = "bgp_shamer"
    TWITTER_LEN = 280


    #####################
    # BOGON IP SETTINGS #
    #####################

    BOGONS_V4 = [
        "0.0.0.0/8", # RFC 1700
        "10.0.0.0/8", # RFC 1918 
        "100.64.0.0/10", # RFC 6598
        "127.0.0.0/8", # RFC 6890
        "169.254.0.0/16", # RFC 6890
        "172.16.0.0/12", # RFC 1918 
        "192.0.0.0/29", # RFC 6333
        "192.0.2.0/24",# RFC 5737 IPv4
        "192.88.99.0/24", # RFC 3068
        "192.168.0.0/16", #RFC 1918 
        "198.18.0.0/15", # RFC 2544 
        "198.51.100.0/24",# RFC 5737 IPv4
        "203.0.113.0/24",# RFC 5737 IPv4
        "224.0.0.0/4", # RFC 5771
        "240.0.0.0/4", # RFC 6890
    ]

    BOGONS_V6 = [
        "::/8", # RFC 4291
        "0100::/64", # RFC 6666
        "2001:2::/48", # RFC 5180
        "2001:10::/28", # RFC 4843
        "2001:db8::/32", # RFC 3849
        "2002::/16", # RFC 7526
        "3ffe::/16", # RFC 3701
        "fc00::/7", # RFC 4193
        "fe00::/9", # IETF Reserved
        "fe80::/10", # RFC 4291
        "fec0::/10", # RFC 3879
        "ff00::/8", # RFC 4291
     ];


    ########################
    # MRT ARCHIVE SETTINGS #
    ########################

    # Base dir to save MRT files to
    DL_DIR = os.path.join(BASE_DIR, "downloads/")

    # Temporary directory to split MRT files into
    SPLIT_DIR = "/tmp/" # Set to None to disable

    # Default interval for downloading and parsing new MRT files (seconds)
    DFT_INTERVAL = 3600

    # Locations of popular MRT dump archives:
    MRT_ARCHIVES = []
    """
    If the machine running this code is in a different timezone to the MRT
    archive, an additional offset in hous is required. A negative int means
    "hours in the future", a positive int means "hours in the past". These are
    the RIB_OFFSET and UPD_INTERVAL values below.
    """

    """
    Transit session from @LukaszBromirski
    RIB dumps are every 1 hour. RIB dumps are disabled!
    RIB dump example: http://192.168.58.8/lukasz/ribs/20211222.0600.dump
    UPDATE dumps are every 10 minutes
    UPDATE dump example: http://192.168.58.8/lukasz/updates/20211222.0600.dump
    """
    MRT_ARCHIVES.append(
        {
            "TYPE": "AS57355",
            "NAME": "AS57355-Lukasz",
            "ENABLED": True,
            "BASE_URL": "http://192.168.58.8:8000/lukasz/",
            "RIB_URL": "/rib/",
            "UPD_URL": "/update/",
            "MRT_EXT": "dump",
            "MRT_DIR": os.path.join(DL_DIR, "AS57355/"),
            "RIB_GLOB": "*.dump",
            "UPD_GLOB": "*.dump",
            "RIB_KEY": "AS57355Z_RIB",
            "UPD_KEY": "AS57355_UPD",
            "RIB_INTERVAL": 60,
            "UPD_INTERVAL": 10,
            "RIB_OFFSET": 0,
            "UPD_OFFSET": 1,
            "RIB_PREFIX": "rib.",
            "UPD_PREFIX": "",
        }
    )

    """
    Route-Views London
    RIB dumps are every 2 hours
    RIB dump example: http://archive.routeviews.org/route-views.linx/bgpdata/2021.12/RIBS/rib.20211222.0600.bz2
    UPDATE dumps are every 15 minutes
    UPDATE dump example: http://archive.routeviews.org/route-views.linx/bgpdata/2021.12/UPDATES/updates.20211222.0600.bz2
    """
    MRT_ARCHIVES.append(
        {
            "TYPE": "RV",
            "NAME": "RV_LINX",
            "ENABLED": False,
            "BASE_URL": "http://archive.routeviews.org/route-views.linx/bgpdata/",
            "RIB_URL": "/RIBS/",
            "UPD_URL": "/UPDATES/",
            "MRT_EXT": "bz2",
            "MRT_DIR": os.path.join(DL_DIR, "LINX/"),
            "RIB_GLOB": "rib.*bz2",
            "UPD_GLOB": "updates.*bz2",
            "RIB_KEY": "RV_LINX_RIB",
            "UPD_KEY": "RV_LINX_UPD",
            "RIB_INTERVAL": 120,
            "UPD_INTERVAL": 15,
            "RIB_OFFSET": 0,
            "UPD_OFFSET": 1,
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
    MRT_ARCHIVES.append(
        {
            "TYPE": "RV",
            "NAME": "RV_SYDNEY",
            "ENABLED": False,
            "BASE_URL": "http://archive.routeviews.org/route-views.sydney/bgpdata/",
            "RIB_URL": "/RIBS/",
            "UPD_URL": "/UPDATES/",
            "MRT_EXT": "bz2",
            "MRT_DIR": os.path.join(DL_DIR, "SYDNEY/"),
            "RIB_GLOB": "rib.*bz2",
            "UPD_GLOB": "updates.*bz2",
            "RIB_KEY": "RV_SYDNEY_RIB",
            "UPD_KEY": "RV_SYDNEY_UPD",
            "RIB_INTERVAL": 120,
            "UPD_INTERVAL": 15,
            "RIB_OFFSET": 0,
            "UPD_OFFSET": 1,            
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
    MRT_ARCHIVES.append(
        {
            "TYPE": "RIPE",
            "NAME": "RCC_23",
            "ENABLED": False,
            "BASE_URL": "https://data.ris.ripe.net/rrc23/",
            "RIB_URL": "/",
            "UPD_URL": "/",
            "MRT_EXT": "gz",
            "MRT_DIR": os.path.join(DL_DIR, "RCC23/"),
            "RIB_GLOB": "bview.*gz",
            "UPD_GLOB": "updates.*gz",
            "RIB_KEY": "RCC_23_RIB",
            "UPD_KEY": "RCC_23_UPD",
            "RIB_INTERVAL": 480,
            "UPD_INTERVAL": 5,
            "RIB_OFFSET": 0,
            "UPD_OFFSET": 1,
            "RIB_PREFIX": "bview.",
            "UPD_PREFIX": "updates.",
        }
    )

    """
    RRC24 Uruguay
    RIB dumps are every 8 hours
    RIB dump example: https://data.ris.ripe.net/rrc24/2021.12/bview.20211208.1600.gz
    UPDATE dumps are every 5 minutes
    UPDATE dump example: https://data.ris.ripe.net/rrc24/2021.12/updates.20211231.1245.gz
    """
    MRT_ARCHIVES.append(
        {
            "TYPE": "RIPE",
            "NAME": "RCC_24",
            "ENABLED": False,
            "BASE_URL": "https://data.ris.ripe.net/rrc24/",
            "RIB_URL": "/",
            "UPD_URL": "/",
            "MRT_EXT": "gz",
            "MRT_DIR": os.path.join(DL_DIR, "RCC24/"),
            "RIB_GLOB": "bview.*gz",
            "UPD_GLOB": "updates.*gz",
            "RIB_KEY": "RCC_24_RIB",
            "UPD_KEY": "RCC_24_UPD",
            "RIB_INTERVAL": 480,
            "UPD_INTERVAL": 5,
            "RIB_OFFSET": 0,
            "UPD_OFFSET": 1,
            "RIB_PREFIX": "bview.",
            "UPD_PREFIX": "updates.",
        }
    )
