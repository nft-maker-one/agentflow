"""Command-line interface — ``agentkit ...``.

The Typer app is exported as :data:`agentkit.cli.main.app` and
registered in ``pyproject.toml``'s ``[project.scripts]`` table so
installing the package gives users an ``agentkit`` shell command.
"""

from agentkit.cli.main import app

__all__ = ["app"]
