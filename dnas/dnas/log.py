import logging
import os
import typing

from dnas.config import config as cfg


class log:
    """
    Class to standardise logging across all DNAS scripts.
    """

    @staticmethod
    def setup(log_src: str, log_path: str, debug: bool = False) -> None:
        """
        Set up logging in a standard format.
        """
        if not log_src or not log_path:
            raise ValueError(
                f"Missing required arguments: log_src={log_src}, "
                f"log_path={log_path}"
            )

        os.makedirs(os.path.dirname(cfg.LOG_DIR), exist_ok=True)
        if debug:
            logging.basicConfig(
                format=cfg.LOG_DEBUG,
                level=logging.DEBUG,
                handlers=[
                    logging.FileHandler(log_path, mode=cfg.LOG_MODE),
                    logging.StreamHandler(),
                ],
            )
        else:
            logging.basicConfig(
                format=cfg.LOG_STANDARD,
                level=logging.INFO,
                handlers=[
                    logging.FileHandler(log_path, mode=cfg.LOG_MODE),
                    logging.StreamHandler(),
                ],
            )

        logging.info(
            f"Starting logging for {log_src} at level "
            f"{logging.getLevelName(logging.getLogger().getEffectiveLevel())}"
        )
