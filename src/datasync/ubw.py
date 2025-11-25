import dlt
import typer
from dlt.sources.rest_api import rest_api_source

from .settings import UBW_BASE_URL, UBW_BASIC_AUTH, log

app = typer.Typer()


@app.command()
def run(
    base_url: str = UBW_BASE_URL,
    auth: str = UBW_BASIC_AUTH,
):
    log.info(base_url)
    source = rest_api_source(
        {
            "client": {
                "base_url": base_url,
                "headers": {
                    "Authorization": f"Basic {auth}",
                    "Accept": "application/json",
                },
            },
            "resources": [
                {
                    "name": "budget",
                    "endpoint": {
                        "path": "objects/ninaprojectsbudmdapis",
                        "paginator": "single_page",
                    },
                },
                {
                    "name": "projects",
                    "endpoint": {
                        "path": "objects/ninaprojectsmdapis",
                        "paginator": "single_page",
                    },
                },
                {
                    "name": "resources",
                    "endpoint": {
                        "path": "objects/ninaprojectresmdapis",
                        "paginator": "single_page",
                    },
                },
                {
                    "name": "units",
                    "endpoint": {
                        "path": "objects/ninaorgunitmdapis",
                        "paginator": "single_page",
                    },
                },
            ],
        }
    )

    pipeline = dlt.pipeline(
        pipeline_name="ubw",
        destination="duckdb",
        dataset_name="main",
    )

    log.info(pipeline.run(source))


if __name__ == "__main__":
    app()
