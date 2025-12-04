import datetime
import pathlib

import dlt
import typer
from dlt.destinations.impl.filesystem.factory import filesystem
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

from .settings import (
    INSTITUTION_CODE,
    NVA_BASE_URL,
    NVA_DUCKDB_NAME,
    log,
)

app = typer.Typer()


# {
#     "name": "projects",
#     "endpoint": {
#         "path": f"cristin/organization/{institution_code}/projects",  # noqa: E501
#         "data_selector": "hits",
#     },
# }
# if projects
# else None,
# {
#     "name": "persons",
#     "endpoint": {
#         "path": f"cristin/organization/{institution_code}/persons",  # noqa: E501
#         "data_selector": "hits",
#     },
# }
# if persons
# else None,
# {
#     "name": "categories",
#     "endpoint": {
#         "path": "cristin/category/project",
#         "data_selector": "hits",
#     },
# }
# if categories
# else None,
# {
#     "name": "funding_sources",
#     "endpoint": {
#         "path": "cristin/funding-sources",
#         "data_selector": "hits",
#     },
# }
# if funding_sources
# else None,


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
):
    @dlt.source()
    def nva():
        client = RESTClient(
            base_url=base_url,
            paginator=JSONLinkPaginator(next_url_path="nextResults"),
            data_selector="hits",
        )

        def get_resources():
            for year in set(range(2025, datetime.datetime.now().year + 1)):
                log.debug("Fetching resources for year", year=year)
                yield from client.paginate(
                    "search/resources",
                    method="get",
                    params={
                        "unit": institution_code,
                        "publicationYearSince": year,
                        "publicationYearBefore": year + 1,
                    },
                )

        yield dlt.resource(
            get_resources(),
            name="resources",
            write_disposition="replace",
            max_table_nesting=1,
        )

    pipeline = dlt.pipeline(
        pipeline_name=duckdb_name,
        destination=filesystem(
            bucket_url="file://"
            + str(pathlib.Path(__name__).absolute().parent / "data"),
            layout="{table_name}.{ext}",
        ),
        dataset_name="main",
        progress="log",
    )

    log.info(
        pipeline.run(nva(), write_disposition="replace", loader_file_format="parquet")
    )


if __name__ == "__main__":
    app()
