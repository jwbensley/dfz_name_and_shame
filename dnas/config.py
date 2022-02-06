class config:
    """
    Class to store global config options.
    """

    # Application root for everything (logging, downloads, tmp files, etc.)
    BASE_DIR = "/opt/dnas/"

    # Base dir to save MRT files to
    DL_DIR = BASE_DIR + "/downloads/"

    # Twitter username
    twitter_user = "bgp_shamer"

    """
    If the machine running this code is in a different timezone to the
    route-views MRT archives, an additional offset in hous is required.
    A negative int is "hours in the future", a positive int is
    "hours in the past".
    """
    RV_RIB_OFFSET = 0
    RV_UPD_OFFSET = 1
    RCC_RIB_OFFSET = 0
    RCC_UPD_OFFSET = 1


    # Locations of popular MRT dump archives:
    MRT_ARCHIVES = []

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
            "ENABLED": True,
            "BASE_URL": "http://archive.routeviews.org/route-views.linx/bgpdata/",
            "RIB_URL": "/RIBS/rib.",
            "UPD_URL": "/UPDATES/updates.",
            "MRT_EXT": ".bz2",
            "MRT_DIR": DL_DIR + "/LINX/",
            "RIB_GLOB": "rib.*bz2",
            "UPD_GLOB": "updates.*bz2",
            "RIB_KEY": "RV_LINX_RIB",
            "UPD_KEY": "RV_LINX_UPD",
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
            "RIB_URL": "/RIBS/rib.",
            "UPD_URL": "/UPDATES/updates.",
            "MRT_EXT": ".bz2",
            "MRT_DIR": DL_DIR + "/SYDNEY/",
            "RIB_GLOB": "rib.*bz2",
            "UPD_GLOB": "updates.*bz2",
            "RIB_KEY": "RV_SYDNEY_RIB",
            "UPD_KEY": "RV_SYDNEY_UPD",
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
            "RIB_URL": "/bview.",
            "UPD_URL": "/updates.",
            "MRT_EXT": ".gz",
            "MRT_DIR": DL_DIR + "/RCC23/",
            "RIB_GLOB": "bview.*gz",
            "UPD_GLOB": "updates.*gz",
            "RIB_KEY": "RCC_23_RIB",
            "UPD_KEY": "RCC_23_UPD",
        }
    )

    """
    RRC24 LACNIC
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
            "RIB_URL": "/bview.",
            "UPD_URL": "/updates.",
            "MRT_EXT": ".gz",
            "MRT_DIR": DL_DIR + "/RCC24/",
            "RIB_GLOB": "bview.*gz",
            "UPD_GLOB": "updates.*gz",
            "RIB_KEY": "RCC_24_RIB",
            "UPD_KEY": "RCC_24_UPD",
        }
    )
