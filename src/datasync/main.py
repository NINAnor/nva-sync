#!/usr/bin/env python3

"""Main script."""

import typer

from . import nva

app = typer.Typer()
app.add(nva.app)

if __name__ == "__main__":
    app()
