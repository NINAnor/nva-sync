import dlt
import typer
from dlt.sources.rest_api import rest_api_source

from .settings import (
    INSTITUTION_CODE,
    NVA_BASE_URL,
    NVA_DUCKDB_NAME,
    log,
)

app = typer.Typer()


def dlt_source(
    year,
    resources,
    projects,
    persons,
    categories,
    funding_sources,
    base_url,
    institution_code,
):
    if year is None:
        log.info("No year specified, syncing all available years.")
        resource_params = {
            "unit": institution_code,
        }
    else:
        log.info(f"Syncing data for year: {year}")
        resource_params = {
            "unit": institution_code,
            "publicationYearSince": year,
            "publicationYearBefore": year + 1,
        }
    return rest_api_source(
        {
            "client": {
                "base_url": base_url,
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
                            "max_table_nesting": 0,
                            "endpoint": {
                                "path": "search/resources",
                                "data_selector": "hits",
                                "params": {
                                    **resource_params,
                                },
                            },
                        }
                        if resources
                        else None,
                        {
                            "name": "projects",
                            "max_table_nesting": 0,
                            "endpoint": {
                                "path": f"cristin/organization/{institution_code}/projects",  # noqa: E501
                                "data_selector": "hits",
                            },
                        }
                        if projects
                        else None,
                        {
                            "name": "persons",
                            "max_table_nesting": 0,
                            "endpoint": {
                                "path": f"cristin/organization/{institution_code}/persons",  # noqa: E501
                                "data_selector": "hits",
                            },
                        }
                        if persons
                        else None,
                        {
                            "name": "categories",
                            "max_table_nesting": 0,
                            "endpoint": {
                                "path": "cristin/category/project",
                                "data_selector": "hits",
                            },
                        }
                        if categories
                        else None,
                        {
                            "name": "funding_sources",
                            "max_table_nesting": 0,
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


@app.command()
def run(
    resources: bool = False,
    projects: bool = False,
    persons: bool = False,
    categories: bool = False,
    funding_sources: bool = False,
    base_url: str = NVA_BASE_URL,
    duckdb_name: str = NVA_DUCKDB_NAME,
    institution_code: str = INSTITUTION_CODE,
    year: int = None,
):
    pipeline = dlt.pipeline(
        pipeline_name=duckdb_name,
        destination="duckdb",
        dataset_name="main",
    )

    source = dlt_source(
        year=year,
        resources=resources,
        projects=projects,
        persons=persons,
        categories=categories,
        funding_sources=funding_sources,
        base_url=base_url,
        institution_code=institution_code,
    )
    log.info(pipeline.run(source))


if __name__ == "__main__":
    app()
