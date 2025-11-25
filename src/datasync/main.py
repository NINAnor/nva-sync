#!/usr/bin/env python3

"""Main script."""

import typer

from . import nva, ubw

app = typer.Typer()
app.add_typer(nva.app, name="nva")
app.add_typer(ubw.app, name="ubw")

if __name__ == "__main__":
    app()
