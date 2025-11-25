import pathlib

import environ

from .logger import configure_logger, logging

env = environ.Env()
BASE_DIR = pathlib.Path(__file__).parent
environ.Env.read_env(str(BASE_DIR / ".env"))

log = configure_logger(
    logging.DEBUG if env.bool("DEBUG", default=False) else logging.INFO
)

# NVA default env

NVA_BASE_URL = env("BASE_URL", default="https://api.test.nva.aws.unit.no/")
NVA_DUCKDB_NAME = env("DUCKDB_FILE_NAME", default="nva_sync")
NVA_INSTITUION_CODE = env("INSTITUTION_CODE", default="7511.0.0.0")
