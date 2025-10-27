#!/usr/bin/env python3

"""Main script."""

import logging
import pathlib

import click
import dlt
import duckdb
import environ
from dlt.sources.rest_api import rest_api_source

env = environ.Env()
BASE_DIR = pathlib.Path(__file__).parent
environ.Env.read_env(str(BASE_DIR / ".env"))

DEBUG = env.bool("DEBUG", default=False)

logging.basicConfig(level=(logging.DEBUG if DEBUG else logging.INFO))

logger = logging.getLogger(__name__)

BASE_URL = env("BASE_URL", default="https://api.test.nva.aws.unit.no/")
DUCKDB_NAME = env("DUCKDB_FILE_NAME", default="nva_sync")
INSTITUION_CODE = env("INSTITUTION_CODE", default="7511.0.0.0")


@click.command()
@click.option("--incremental", is_flag=True)
def start(incremental) -> None:
    incremental_resource = {}

    if incremental:
        con = duckdb.connect(DUCKDB_NAME + ".duckdb")
        result = con.execute(
            "select modified_date from nva.resources order by modified_date asc limit 1"
        ).fetchone()
        con.close()

        if last_date := len(result) and result[0]:
            incremental_resource = {
                "incremental": {
                    "cursor_path": "modifiedDate",
                    "initial_value": last_date.isoformat(),
                },
                "params": {
                    "modified_since": "{incremental.start_value}",
                },
            }

    source = rest_api_source(
        {
            "client": {
                "base_url": BASE_URL,
                "paginator": {
                    "type": "json_link",
                    "next_url_path": "nextResults",
                },
            },
            "resources": [
                {
                    "name": "resources",
                    "endpoint": {
                        "path": "search/resources",
                        "data_selector": "hits",
                        "params": {
                            "institution": INSTITUION_CODE,
                        },
                    }
                    | incremental_resource,  # merge incremental settings
                },
                {
                    "name": "projects",
                    "endpoint": {
                        "path": "cristin/project",
                        "data_selector": "hits",
                        "params": {
                            "organization": f"{BASE_URL}/cristin/organization/{INSTITUION_CODE}",  # noqa: E501
                        },
                    },
                },
            ],
        }
    )

    pipeline = dlt.pipeline(
        pipeline_name=DUCKDB_NAME,
        destination="duckdb",
        dataset_name="main",
    )

    load_info = pipeline.run(source)
    print(load_info)


if __name__ == "__main__":
    start()
