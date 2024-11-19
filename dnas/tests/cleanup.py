import os

import pytest


def test_cleanup() -> None:
    for file in [
        "/opt/dnas_data/downloads/RRC1/rrc01.updates.20100827.0840.gz",
        "/opt/dnas_data/downloads/RRC1/rrc01.updates.20241001.0055.gz",
        "/opt/dnas_data/downloads/RRC23/rrc23.updates.20220421.0200.gz",
        "/opt/dnas_data/downloads/RRC23/rrc23.updates.20220501.2305.gz",
        "/opt/dnas_data/downloads/SYDNEY/sydney.updates.20220601.0230.bz2",
        "/opt/dnas_data/downloads/SYDNEY/sydney.updates.20220601.0415.bz2",
    ]:
        if os.path.exists(file):
            print(f"Doing to delete: {file}")
            os.unlink(file)
