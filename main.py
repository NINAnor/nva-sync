#!/usr/bin/env python3

"""Main script."""

import logging
import pathlib

import click
import dlt
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
@click.option("--resources", is_flag=True)
@click.option("--projects", is_flag=True)
@click.option("--persons", is_flag=True)
@click.option("--categories", is_flag=True)
@click.option("--funding_sources", is_flag=True)
def start(resources, projects, persons, categories, funding_sources) -> None:
    source = rest_api_source(
        {
            "client": {
                "base_url": BASE_URL,
                "paginator": {
                    "type": "json_link",
                    "next_url_path": "nextResults",
                },
            },
            "resources": list(
                filter(
                    lambda r: r is not None,
                    [
                        {
                            "name": "resources",
                            "endpoint": {
                                "path": "search/resources",
                                "data_selector": "hits",
                                "params": {
                                    "institution": INSTITUION_CODE,
                                },
                            },
                        }
                        if resources
                        else None,
                        {
                            "name": "projects",
                            "endpoint": {
                                "path": f"cristin/organization/{INSTITUION_CODE}/projects",  # noqa: E501
                                "data_selector": "hits",
                            },
                        }
                        if projects
                        else None,
                        {
                            "name": "persons",
                            "endpoint": {
                                "path": f"cristin/organization/{INSTITUION_CODE}/persons",  # noqa: E501
                                "data_selector": "hits",
                            },
                        }
                        if persons
                        else None,
                        {
                            "name": "categories",
                            "endpoint": {
                                "path": "cristin/category/project",
                                "data_selector": "hits",
                            },
                        }
                        if categories
                        else None,
                        {
                            "name": "funding_sources",
                            "endpoint": {
                                "path": "cristin/funding-sources",
                                "data_selector": "hits",
                            },
                        }
                        if funding_sources
                        else None,
                    ],
                )
            ),
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
