import pathlib

import environ

from .logger import configure_logger, logging

env = environ.Env()
BASE_DIR = pathlib.Path(__file__).parent.parent.parent

environ.Env.read_env(str(BASE_DIR / ".env"))

log = configure_logger(
    logging.DEBUG if env.bool("DEBUG", default=False) else logging.INFO
)

# NVA default env

NVA_BASE_URL = env("NVA_BASE_URL", default="https://api.test.nva.aws.unit.no/")
NVA_DUCKDB_NAME = env("NVA_DUCKDB_FILE_NAME", default="nva_sync")
NVA_INSTITUION_CODE = env("NVA_INSTITUTION_CODE", default="7511.0.0.0")

# UBW
UBW_BASE_URL = env("UBW_BASE_URL", default="")
UBW_DUCKDB_NAME = env("UBW_DUCKDB_FILE_NAME", default="ubw_sync")
UBW_BASIC_AUTH = env("UBW_BASIC_AUTH", default="")
