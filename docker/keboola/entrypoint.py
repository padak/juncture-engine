"""Entrypoint for the Keboola component image.

Reads /data/config.json, invokes :class:`juncture.keboola.KeboolaRunner`,
exits 0 on success, 1 on any failure.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from juncture.keboola import KeboolaRunner

logging.basicConfig(
    level=os.environ.get("JUNCTURE_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
log = logging.getLogger("juncture.keboola.entrypoint")


def main() -> int:
    config_path = Path(os.environ.get("JUNCTURE_KEBOOLA_CONFIG", "/data/config.json"))
    if not config_path.exists():
        log.error("Keboola config not found at %s", config_path)
        return 2

    runner = KeboolaRunner.from_keboola_config_json(config_path)
    ok = runner.run()
    if not ok:
        log.error("Juncture run reported failures; see the report above.")
        return 1
    log.info("Juncture run completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
