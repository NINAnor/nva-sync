import dlt
import typer
from dlt.sources.rest_api import rest_api_source

from .settings import NVA_BASE_URL, NVA_DUCKDB_NAME, NVA_INSTITUION_CODE, log

app = typer.Typer()


@app.command()
def nva(
    resources: bool = False,
    projects: bool = False,
    persons: bool = False,
    categories: bool = False,
    funding_sources: bool = False,
    base_url: str = NVA_BASE_URL,
    duckdb_name: str = NVA_DUCKDB_NAME,
    institution_code: str = NVA_INSTITUION_CODE,
):
    source = rest_api_source(
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
                            "endpoint": {
                                "path": "search/resources",
                                "data_selector": "hits",
                                "params": {
                                    "institution": institution_code,
                                },
                            },
                        }
                        if resources
                        else None,
                        {
                            "name": "projects",
                            "endpoint": {
                                "path": f"cristin/organization/{institution_code}/projects",  # noqa: E501
                                "data_selector": "hits",
                            },
                        }
                        if projects
                        else None,
                        {
                            "name": "persons",
                            "endpoint": {
                                "path": f"cristin/organization/{institution_code}/persons",  # noqa: E501
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
        pipeline_name=duckdb_name,
        destination="duckdb",
        dataset_name="main",
    )

    log.info(pipeline.run(source))


if __name__ == "__main__":
    app()
