

class config:

    # Where to save MRT files to
    DL_DIR = "/opt/dnas/downloads/"

    """
    If the machine running this code is in a different timezone to the RV
    archives, an additional offset in hous is required. A negative int is
    "hours in the future", a positive int is "hours in the past".
    """
    RV_RIB_OFFSET = 0
    RV_UPD_OFFSET = 1
    RCC_RIB_OFFSET = 0
    RCC_UPD_OFFSET = 1


    # Locations of popular MRT dump archives:

    """
    Route-Views London
    RIB dumps are every 2 hours
    RIB dump example: http://archive.routeviews.org/route-views.linx/bgpdata/2021.12/RIBS/rib.20211222.0600.bz2
    UPDATE dumps are every 15 minutes
    UPDATE dump example: http://archive.routeviews.org/route-views.linx/bgpdata/2021.12/UPDATES/updates.20211222.0600.bz2
    """
    RV_LINX_BASE = "http://archive.routeviews.org/route-views.linx/bgpdata/"
    RV_LINX_RIBS = "/RIBS/rib."
    RV_LINX_UPDS = "/UPDATES/updates."
    RV_LINX_FXT = ".bz2"
    RV_LINX_DIR = DL_DIR + "/LINX/"
    RV_LINX_GLOB = RV_LINX_DIR + "updates.*bz2"
    RV_LINX_RIB_KEY = "RV_LINX_RIB"
    RV_LINX_UPD_KEY = "RV_LINX_UPD"

    """
    Route-Views Sydnex
    RIB dumps are every 2 hours
    RIB dump example: http://archive.routeviews.org/route-views.sydney/bgpdata/2021.12/RIBS/rib.20211201.0600.bz2
    UPDATE dumps are every 15 minutes
    UPDATE dump example: http://archive.routeviews.org/route-views.sydney/bgpdata/2021.12/UPDATES/updates.20211201.0030.bz2
    """
    RV_SYDNEY_BASE = "http://archive.routeviews.org/route-views.sydney/bgpdata/"
    RV_SYDNEY_RIBS = "/RIBS/rib."
    RV_SYDNEY_UPDS = "/UPDATES/updates."
    RV_SYDNEY_FXT = ".bz2"
    RV_SYDNEY_DIR = DL_DIR + "/SYDNEY/"
    RV_SYDNEY_GLOB = RV_SYDNEY_DIR + "updates.*bz2"
    RV_SYDNEY_RIB_KEY = "RV_SYDNEY_RIB"
    RV_SYDNEY_UPD_KEY = "RV_SYDNEY_UPD"

    """
    RRC23 Singapore
    RIB dumps are every 8 hours
    RIB dump example: https://data.ris.ripe.net/rrc23/2021.12/bview.20211206.1600.gz
    UPDATE dumps are every 5 minutes
    UPDATE dump example: https://data.ris.ripe.net/rrc23/2021.12/updates.20211231.2335.gz
    """
    RCC_23_BASE = "https://data.ris.ripe.net/rrc23/"
    RCC_23_RIBS = "/bview."
    RCC_23_UPDS = "/updates."
    RCC_23_FXT = ".gz"
    RCC_23_DIR = DL_DIR + "/RCC23/"
    RCC_23_GLOB = RCC_23_DIR + "updates.*gz"
    RCC_23_RIB_KEY = "RCC_23_RIB"
    RCC_23_UPD_KEY = "RCC_23_UPD"

    """
    RRC24 LACNIC
    RIB dumps are every 8 hours
    RIB dump example: https://data.ris.ripe.net/rrc24/2021.12/bview.20211208.1600.gz
    UPDATE dumps are every 5 minutes
    UPDATE dump example: https://data.ris.ripe.net/rrc24/2021.12/updates.20211231.1245.gz
    """
    RCC_24_BASE = "https://data.ris.ripe.net/rrc24/"
    RCC_24_RIBS = "/bview."
    RCC_24_UPDS = "/updates."
    RCC_24_FXT = ".gz"
    RCC_24_DIR = DL_DIR + "/RCC24/"
    RCC_24_GLOB = RCC_24_DIR + "updates.*gz"
    RCC_24_RIB_KEY = "RCC_24_RIB"
    RCC_24_UPD_KEY = "RCC_24_UPD"
