import os


class config:
    """
    Class to store global config options.
    """

    #################
    # BASE SETTINGS #
    #################

    """
    Data root (for logging, downloads, tmp files, etc.) with trailing slash:
    """
    DATA_DIR = "/opt/dnas_data/"

    """
    The time format used for generating new timestamps and parsing existing
    timestamps. The format below is the same format used by the MRT archives.
    """
    TIME_FORMAT = "%Y%m%d.%H%M"
    DAY_FORMAT = "%Y%m%d"

    # JSON indent when exporting MRT entry to JSON
    MRT_ENTRY_JSON_INDENT = 2
    # JSON indent when exporting MRT stats to JSON
    MRT_STATS_JSON_INDENT = 2

    # Log mode, 'a'ppend or over'w'rite
    LOG_MODE = "a"
    # Standard logging format
    LOG_STANDARD = "%(asctime)s|%(levelname)s|%(message)s"
    # Debugging logging formart
    LOG_DEBUG = (
        "%(asctime)s|%(levelname)s|%(process)d|%(funcName)s|%(message)s"
    )
    # Log directory for all logs
    LOG_DIR = os.path.join(DATA_DIR, "logs/")
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
    # Logging from the script: mrt_test.py
    LOG_TESTER = os.path.join(LOG_DIR, "mrt_test.log")
    # Logging from script: tweet.py
    LOG_TWITTER = os.path.join(LOG_DIR, "tweet.log")
    # Logging from script: update_asn_allocations.py
    LOG_UPDATE_ASN = os.path.join(LOG_DIR, "update_asn.log")

    ###########################
    # ASN ALLOCATION SETTINGS #
    ###########################

    # URL of ASN allocation stats
    asn_allocation_url = (
        "https://www.iana.org/assignments/as-numbers/as-numbers-2.csv"
    )
    ASN_DATA = os.path.join(DATA_DIR, "asn_data/")
    # Output file
    asn_stats_file = os.path.join(ASN_DATA, "iana-32bit-asns.csv")
    # Allocated ASNs list
    unallocated_asns_file = os.path.join(ASN_DATA, "unallocated-asns.txt")

    ###################
    # PARSER SETTINGS #
    ###################

    """
    Max MRT file size to parse using multiple Python processes, files larger
    than this value (in bytes) will be parsed without using multiprocessing
    (a single Python process / on a single core).
    """
    MAX_MRT_SIZE = 60000000

    """
    Min MRT file size to parse. Files less than this size (in bytes) are
    considered invalid. The minimum MRT header size is 64 bytes:
    """
    MIN_MRT_SIZE = 64

    # Undefined MED / default when missing
    MISSING_MED = -1

    ################
    # GIT SETTINGS #
    ################

    # Git clone URL
    GIT_STAT_CLONE_URL = "git@github.com:DFZ-Name-and-Shame/dnas_stats.git"

    # Remote git details
    GIT_STAT_BASE_URL = (
        "https://github.com/DFZ-Name-and-Shame/dnas_stats/tree/main/"
    )

    # Local git repo details
    GIT_BASE = os.path.join(DATA_DIR, "dnas_stats/")
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
        "0.0.0.0/8",  # RFC 1700
        "10.0.0.0/8",  # RFC 1918
        "100.64.0.0/10",  # RFC 6598
        "127.0.0.0/8",  # RFC 6890
        "169.254.0.0/16",  # RFC 6890
        "172.16.0.0/12",  # RFC 1918
        "192.0.0.0/29",  # RFC 6333
        "192.0.2.0/24",  # RFC 5737 IPv4
        "192.88.99.0/24",  # RFC 3068
        "192.168.0.0/16",  # RFC 1918
        "198.18.0.0/15",  # RFC 2544
        "198.51.100.0/24",  # RFC 5737 IPv4
        "203.0.113.0/24",  # RFC 5737 IPv4
        "224.0.0.0/4",  # RFC 5771
        "240.0.0.0/4",  # RFC 6890
    ]

    BOGONS_V6 = [
        "::/8",  # RFC 4291
        "0100::/64",  # RFC 6666
        "2001:2::/48",  # RFC 5180
        "2001:10::/28",  # RFC 4843
        "2001:db8::/32",  # RFC 3849
        "2002::/16",  # RFC 7526
        "3ffe::/16",  # RFC 3701
        "fc00::/7",  # RFC 4193
        "fe00::/9",  # IETF Reserved
        "fe80::/10",  # RFC 4291
        "fec0::/10",  # RFC 3879
        "ff00::/8",  # RFC 4291
    ]

    ########################
    # MRT ARCHIVE SETTINGS #
    ########################

    # Base dir to save MRT files to
    DL_DIR = os.path.join(DATA_DIR, "downloads/")

    # Temporary directory to split MRT files into
    SPLIT_DIR = "/tmp/"  # Set to None to disable MRT splitting

    # Default interval for downloading and parsing new MRT files (seconds)
    DFT_INTERVAL = 3600

    # Locations of popular MRT dump archives:
    MRT_ARCHIVES = []
    """
    If the machine running this code is in a different timezone to the MRT
    archive, an additional offset in hours is required. A negative int means
    "hours in the future", a positive int means "hours in the past". These are
    the RIB_OFFSET and UPD_INTERVAL values below.
    """

    # DISABLED AS PART OF https://github.com/jwbensley/dfz_name_and_shame/issues/135
    # This is an example of how to configure a local BIRD instance which is dumping MRTs
    """
    Transit session from @LukaszBromirski
    RIB dumps are every 1 hour. RIB dumps are disabled!
    RIB dump example: http://172.17.0.1/as57355/ribs/20211222.0600.dump
    UPDATE dumps are every 10 minutes
    UPDATE dump example: http://172.17.0.1/as57355/updates/20211222.0600.dump
    """
    MRT_ARCHIVES.append(
        {
            "TYPE": "AS57355",
            "NAME": "AS57355-Lukasz",
            "ENABLED": False,
            "BASE_URL": "http://172.17.0.1:8000/as57355/",
            "RIB_URL": "/rib/",
            "UPD_URL": "/updates/",
            "MRT_EXT": "mrt",
            "MRT_DIR": os.path.join(DL_DIR, "AS57355/"),
            "RIB_GLOB": "*.mrt",
            "UPD_GLOB": "*.mrt",
            "RIB_KEY": "AS57355_RIB",
            "UPD_KEY": "AS57355_UPD",
            "RIB_INTERVAL": 60,
            "UPD_INTERVAL": 5,
            "RIB_OFFSET": 0,  # Offset from UTC0
            "UPD_OFFSET": 0,  # Offset from UTC0
            "RIB_PREFIX": "rib.",
            "UPD_PREFIX": "",
            "STRIP_COMM": "",  # To strip the communities from the IXP where the collector is hosted
        }
    )

    # DISABLED AS PART OF https://github.com/jwbensley/dfz_name_and_shame/issues/135
    # This is an example of how to configure a RouteViews collector
    """
    Route-Views London/LINX
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
            "RIB_OFFSET": 60,
            "UPD_OFFSET": 60,
            "RIB_PREFIX": "rib.",
            "UPD_PREFIX": "updates.",
            "STRIP_COMM": "0:",
        }
    )

    # DISABLED AS PART OF https://github.com/jwbensley/dfz_name_and_shame/issues/135
    """
    Route-Views Sydney/Equinix Sydney
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
            "RIB_OFFSET": 60,
            "UPD_OFFSET": 60,
            "RIB_PREFIX": "rib.",
            "UPD_PREFIX": "updates.",
            "STRIP_COMM": "0:",
        }
    )

    """
    RRC00 Amsterdam Multihop
    RIB dumps are every 8 hours
    RIB dump example: https://data.ris.ripe.net/rrc00/2023.07/bview.20230702.0000.gz
    UPDATE dumps are every 5 minutes
    UPDATE dump example: https://data.ris.ripe.net/rrc00/2023.07/updates.20230701.1540.gz
    """
    MRT_ARCHIVES.append(
        {
            "TYPE": "RIPE",
            "NAME": "RRC_00",
            "ENABLED": True,
            "BASE_URL": "https://data.ris.ripe.net/rrc00/",
            "RIB_URL": "/",
            "UPD_URL": "/",
            "MRT_EXT": "gz",
            "MRT_DIR": os.path.join(DL_DIR, "RRC00/"),
            "RIB_GLOB": "bview.*gz",
            "UPD_GLOB": "updates.*gz",
            "RIB_KEY": "RRC_00_RIB",
            "UPD_KEY": "RRC_00_UPD",
            "RIB_INTERVAL": 480,
            "UPD_INTERVAL": 5,
            "RIB_OFFSET": 60,
            "UPD_OFFSET": 60,
            "RIB_PREFIX": "bview.",
            "UPD_PREFIX": "updates.",
            "STRIP_COMM": "",
        }
    )

    """
    RRC01 London LINX
    RIB dumps are every 8 hours
    RIB dump example: https://data.ris.ripe.net/rrc01/2023.07/bview.20230702.0000.gz
    UPDATE dumps are every 5 minutes
    UPDATE dump example: https://data.ris.ripe.net/rrc01/2023.07/updates.20230701.1540.gz
    """
    MRT_ARCHIVES.append(
        {
            "TYPE": "RIPE",
            "NAME": "RRC_01",
            "ENABLED": True,
            "BASE_URL": "https://data.ris.ripe.net/rrc01/",
            "RIB_URL": "/",
            "UPD_URL": "/",
            "MRT_EXT": "gz",
            "MRT_DIR": os.path.join(DL_DIR, "RRC01/"),
            "RIB_GLOB": "bview.*gz",
            "UPD_GLOB": "updates.*gz",
            "RIB_KEY": "RRC_01_RIB",
            "UPD_KEY": "RRC_01_UPD",
            "RIB_INTERVAL": 480,
            "UPD_INTERVAL": 5,
            "RIB_OFFSET": 60,
            "UPD_OFFSET": 60,
            "RIB_PREFIX": "bview.",
            "UPD_PREFIX": "updates.",
            "STRIP_COMM": "8714:",
        }
    )

    """
    RRC03 Amsterdam AMS-IX
    RIB dumps are every 8 hours
    RIB dump example: https://data.ris.ripe.net/rrc03/2023.07/bview.20230702.0000.gz
    UPDATE dumps are every 5 minutes
    UPDATE dump example: https://data.ris.ripe.net/rrc03/2023.07/updates.20230701.1540.gz
    """
    MRT_ARCHIVES.append(
        {
            "TYPE": "RIPE",
            "NAME": "RRC_03",
            "ENABLED": True,
            "BASE_URL": "https://data.ris.ripe.net/rrc03/",
            "RIB_URL": "/",
            "UPD_URL": "/",
            "MRT_EXT": "gz",
            "MRT_DIR": os.path.join(DL_DIR, "RRC03/"),
            "RIB_GLOB": "bview.*gz",
            "UPD_GLOB": "updates.*gz",
            "RIB_KEY": "RRC_03_RIB",
            "UPD_KEY": "RRC_03_UPD",
            "RIB_INTERVAL": 480,
            "UPD_INTERVAL": 5,
            "RIB_OFFSET": 60,
            "UPD_OFFSET": 60,
            "RIB_PREFIX": "bview.",
            "UPD_PREFIX": "updates.",
            "STRIP_COMM": "6777:",
        }
    )

    """
    RRC12 Frankfurt DE-CIX
    RIB dumps are every 8 hours
    RIB dump example: https://data.ris.ripe.net/rrc12/2023.07/bview.20230702.0000.gz
    UPDATE dumps are every 5 minutes
    UPDATE dump example: https://data.ris.ripe.net/rrc12/2023.07/updates.20230701.1540.gz
    """
    MRT_ARCHIVES.append(
        {
            "TYPE": "RIPE",
            "NAME": "RRC_12",
            "ENABLED": True,
            "BASE_URL": "https://data.ris.ripe.net/rrc12/",
            "RIB_URL": "/",
            "UPD_URL": "/",
            "MRT_EXT": "gz",
            "MRT_DIR": os.path.join(DL_DIR, "RRC12/"),
            "RIB_GLOB": "bview.*gz",
            "UPD_GLOB": "updates.*gz",
            "RIB_KEY": "RRC_12_RIB",
            "UPD_KEY": "RRC_12_UPD",
            "RIB_INTERVAL": 480,
            "UPD_INTERVAL": 5,
            "RIB_OFFSET": 60,
            "UPD_OFFSET": 60,
            "RIB_PREFIX": "bview.",
            "UPD_PREFIX": "updates.",
            "STRIP_COMM": "6695:",
        }
    )

    """
    RRC15 Sao Paulo PTTMetro
    RIB dumps are every 8 hours
    RIB dump example: https://data.ris.ripe.net/rrc15/2021.12/bview.20211206.1600.gz
    UPDATE dumps are every 5 minutes
    UPDATE dump example: https://data.ris.ripe.net/rrc15/2021.12/updates.20211231.2335.gz
    """
    MRT_ARCHIVES.append(
        {
            "TYPE": "RIPE",
            "NAME": "RRC_15",
            "ENABLED": True,
            "BASE_URL": "https://data.ris.ripe.net/rrc15/",
            "RIB_URL": "/",
            "UPD_URL": "/",
            "MRT_EXT": "gz",
            "MRT_DIR": os.path.join(DL_DIR, "RRC15/"),
            "RIB_GLOB": "bview.*gz",
            "UPD_GLOB": "updates.*gz",
            "RIB_KEY": "RRC_15_RIB",
            "UPD_KEY": "RRC_15_UPD",
            "RIB_INTERVAL": 480,
            "UPD_INTERVAL": 5,
            "RIB_OFFSET": 60,
            "UPD_OFFSET": 60,
            "RIB_PREFIX": "bview.",
            "UPD_PREFIX": "updates.",
            "STRIP_COMM": "26162:",
        }
    )

    """
    RRC19 Johannesburg NAPAfrica
    RIB dumps are every 8 hours
    RIB dump example: https://data.ris.ripe.net/rrc19/2021.12/bview.20211206.1600.gz
    UPDATE dumps are every 5 minutes
    UPDATE dump example: https://data.ris.ripe.net/rrc19/2021.12/updates.20211231.2335.gz
    """
    MRT_ARCHIVES.append(
        {
            "TYPE": "RIPE",
            "NAME": "RRC_19",
            "ENABLED": True,
            "BASE_URL": "https://data.ris.ripe.net/rrc19/",
            "RIB_URL": "/",
            "UPD_URL": "/",
            "MRT_EXT": "gz",
            "MRT_DIR": os.path.join(DL_DIR, "RRC19/"),
            "RIB_GLOB": "bview.*gz",
            "UPD_GLOB": "updates.*gz",
            "RIB_KEY": "RRC_19_RIB",
            "UPD_KEY": "RRC_19_UPD",
            "RIB_INTERVAL": 480,
            "UPD_INTERVAL": 5,
            "RIB_OFFSET": 60,
            "UPD_OFFSET": 60,
            "RIB_PREFIX": "bview.",
            "UPD_PREFIX": "updates.",
            "STRIP_COMM": "37195:",
        }
    )

    """
    RRC23 Singapore Equinix-SG
    RIB dumps are every 8 hours
    RIB dump example: https://data.ris.ripe.net/rrc23/2021.12/bview.20211206.1600.gz
    UPDATE dumps are every 5 minutes
    UPDATE dump example: https://data.ris.ripe.net/rrc23/2021.12/updates.20211231.2335.gz
    """
    MRT_ARCHIVES.append(
        {
            "TYPE": "RIPE",
            "NAME": "RRC_23",
            "ENABLED": True,
            "BASE_URL": "https://data.ris.ripe.net/rrc23/",
            "RIB_URL": "/",
            "UPD_URL": "/",
            "MRT_EXT": "gz",
            "MRT_DIR": os.path.join(DL_DIR, "RRC23/"),
            "RIB_GLOB": "bview.*gz",
            "UPD_GLOB": "updates.*gz",
            "RIB_KEY": "RRC_23_RIB",
            "UPD_KEY": "RRC_23_UPD",
            "RIB_INTERVAL": 480,
            "UPD_INTERVAL": 5,
            "RIB_OFFSET": 60,
            "UPD_OFFSET": 60,
            "RIB_PREFIX": "bview.",
            "UPD_PREFIX": "updates.",
            "STRIP_COMM": "24115:",
        }
    )

    """
    RRC25 Amsterdam Multihop
    RIB dumps are every 8 hours
    RIB dump example: https://data.ris.ripe.net/rrc25/2021.12/bview.20211208.1600.gz
    UPDATE dumps are every 5 minutes
    UPDATE dump example: https://data.ris.ripe.net/rrc25/2021.12/updates.20211231.1245.gz
    """
    MRT_ARCHIVES.append(
        {
            "TYPE": "RIPE",
            "NAME": "RRC_25",
            "ENABLED": True,
            "BASE_URL": "https://data.ris.ripe.net/rrc25/",
            "RIB_URL": "/",
            "UPD_URL": "/",
            "MRT_EXT": "gz",
            "MRT_DIR": os.path.join(DL_DIR, "RRC25/"),
            "RIB_GLOB": "bview.*gz",
            "UPD_GLOB": "updates.*gz",
            "RIB_KEY": "RRC_25_RIB",
            "UPD_KEY": "RRC_25_UPD",
            "RIB_INTERVAL": 480,
            "UPD_INTERVAL": 5,
            "RIB_OFFSET": 60,
            "UPD_OFFSET": 60,
            "RIB_PREFIX": "bview.",
            "UPD_PREFIX": "updates.",
            "STRIP_COMM": "",
        }
    )

    ##################################
    # UNIT TEST MRT ARCHIVE SETTINGS #
    ##################################

    """
    Route-Views Sydney/Equinix Sydney
    Used for unit tests only.
    """
    MRT_ARCHIVES.append(
        {
            "TYPE": "RV",
            "NAME": "UNIT_TEST_RV_SYDNEY",
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
            "RIB_OFFSET": 60,
            "UPD_OFFSET": 60,
            "RIB_PREFIX": "rib.",
            "UPD_PREFIX": "updates.",
            "STRIP_COMM": "0:",
        }
    )

    """
    RRC1 London
    Used for unit tests only.
    """
    MRT_ARCHIVES.append(
        {
            "TYPE": "RIPE",
            "NAME": "UNIT_TEST_RRC_1",
            "ENABLED": False,
            "BASE_URL": "https://data.ris.ripe.net/rrc1/",
            "RIB_URL": "/",
            "UPD_URL": "/",
            "MRT_EXT": "gz",
            "MRT_DIR": os.path.join(DL_DIR, "RRC1/"),
            "RIB_GLOB": "bview.*gz",
            "UPD_GLOB": "updates.*gz",
            "RIB_KEY": "RRC_1_RIB",
            "UPD_KEY": "RRC_1_UPD",
            "RIB_INTERVAL": 480,
            "UPD_INTERVAL": 5,
            "RIB_OFFSET": 60,
            "UPD_OFFSET": 60,
            "RIB_PREFIX": "bview.",
            "UPD_PREFIX": "updates.",
            "STRIP_COMM": "8714:",
        }
    )

    """
    RRC23 Singapore
    Used for unit tests only.
    """
    MRT_ARCHIVES.append(
        {
            "TYPE": "RIPE",
            "NAME": "UNIT_TEST_RRC_23",
            "ENABLED": False,
            "BASE_URL": "https://data.ris.ripe.net/rrc23/",
            "RIB_URL": "/",
            "UPD_URL": "/",
            "MRT_EXT": "gz",
            "MRT_DIR": os.path.join(DL_DIR, "RRC23/"),
            "RIB_GLOB": "bview.*gz",
            "UPD_GLOB": "updates.*gz",
            "RIB_KEY": "RRC_23_RIB",
            "UPD_KEY": "RRC_23_UPD",
            "RIB_INTERVAL": 480,
            "UPD_INTERVAL": 5,
            "RIB_OFFSET": 60,
            "UPD_OFFSET": 60,
            "RIB_PREFIX": "bview.",
            "UPD_PREFIX": "updates.",
            "STRIP_COMM": "24115:",
        }
    )
