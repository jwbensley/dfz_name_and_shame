import datetime
import logging

import sys
sys.path.append('./')
from mrt_getter import mrt_getter
from redis_db import redis_db
from config import config as cfg

def main():

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    logging.info("Starting MRT downloader")
    g = mrt_getter()
    rdb = redis_db()

    g.get_range_rv_upd(
        base_url=cfg.RV_LINX_BASE,
        dl_dir=cfg.RV_LINX_DIR,
        file_ext=cfg.RV_LINX_FXT,
        replace=False,
        upd_pfx=cfg.RV_LINX_UPDS,
        start_date="20220102.0000",
        end_date="20220102.2359",
    )

    exit(0)

    g.get_range_rv_upd(
        base_url=cfg.RV_SYDNEY_BASE,
        dl_dir=cfg.RV_SYDNEY_DIR,
        end_date="20220101.0045",
        file_ext=cfg.RV_SYDNEY_FXT,
        replace=False,
        start_date="20220101.0000",
        upd_pfx=cfg.RV_SYDNEY_UPDS,
    )
    
    print(
        g.get_rv_latest_rib(
            base_url=cfg.RV_LINX_BASE,
            dl_dir=cfg.RV_LINX_DIR,
            file_ext=cfg.RV_LINX_FXT,
            replace=False,
            rib_pfx=cfg.RV_LINX_RIBS,
        )
    )

    print(
        g.get_rv_latest_upd(
            base_url=cfg.RV_LINX_BASE,
            dl_dir=cfg.RV_LINX_DIR,
            file_ext=cfg.RV_LINX_FXT,
            replace=False,
            upd_pfx=cfg.RV_LINX_UPDS,
        )
    )

    print(
        g.get_rv_latest_rib(
            base_url=cfg.RV_SYDNEY_BASE,
            dl_dir=cfg.RV_SYDNEY_DIR,
            file_ext=cfg.RV_SYDNEY_FXT,
            replace=False,
            rib_pfx=cfg.RV_SYDNEY_RIBS,
        )
    )

    print(
        g.get_rv_latest_upd(
            base_url=cfg.RV_SYDNEY_BASE,
            dl_dir=cfg.RV_SYDNEY_DIR,
            file_ext=cfg.RV_SYDNEY_FXT,
            replace=False,
            upd_pfx=cfg.RV_SYDNEY_UPDS,
        )
    )


    print(
        g.get_ripe_latest_rib(
            base_url=cfg.RCC_23_BASE,
            dl_dir=cfg.RCC_23_DIR,
            file_ext=cfg.RCC_23_FXT,
            replace=False,
            rib_pfx=cfg.RCC_23_RIBS,
        )
    )

    print(
        g.get_ripe_latest_upd(
            base_url=cfg.RCC_23_BASE,
            dl_dir=cfg.RCC_23_DIR,
            file_ext=cfg.RCC_23_FXT,
            replace=False,
            upd_pfx=cfg.RCC_23_UPDS,
        )
    )


    print(
        g.get_ripe_latest_rib(
            base_url=cfg.RCC_24_BASE,
            dl_dir=cfg.RCC_24_DIR,
            file_ext=cfg.RCC_24_FXT,
            replace=False,
            rib_pfx=cfg.RCC_24_RIBS,
        )
    )

    print(
        g.get_ripe_latest_upd(
            base_url=cfg.RCC_24_BASE,
            dl_dir=cfg.RCC_24_DIR,
            file_ext=cfg.RCC_24_FXT,
            replace=False,
            upd_pfx=cfg.RCC_24_UPDS,
        )
    )

if __name__ == '__main__':
    main()