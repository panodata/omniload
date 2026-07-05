from __future__ import absolute_import

import logging
import os

import colorlog
from colorlog.escape_codes import escape_codes
from sqlalchemy.util import asbool


def setup_logging(
    level=logging.INFO, verbose: bool = False, debug: bool = False, width: int = 36
):
    if os.environ.get("DEBUG"):
        level = logging.DEBUG

    reset = escape_codes["reset"]
    log_format = (
        f"%(asctime)-15s [%(name)-{width}s] "
        f"%(log_color)s%(levelname)-8s:{reset} %(message)s"
    )

    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(log_format))

    logging.basicConfig(format=log_format, level=level, handlers=[handler])

    logging.getLogger("urllib3.connectionpool").setLevel(level)

    if verbose:
        logging.getLogger("omniload").setLevel(logging.DEBUG)

    if debug:
        # Optionally tame SQLAlchemy and PyMongo.
        if asbool(os.environ.get("DEBUG_SQLALCHEMY")):
            logging.getLogger("sqlalchemy").setLevel(level)
        else:
            logging.getLogger("sqlalchemy").setLevel(logging.INFO)
        if asbool(os.environ.get("DEBUG_PYMONGO")):
            logging.getLogger("pymongo").setLevel(level)
        else:
            logging.getLogger("pymongo").setLevel(logging.INFO)

    # logging.getLogger("docker.auth").setLevel(logging.INFO)  # noqa: ERA001
