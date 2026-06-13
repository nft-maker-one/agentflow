"""Tests for the ``agentkit`` CLI entry points."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from agentkit import __version__
from agentkit.cli.main import app


runner = CliRunner()


# ----------------------------------------------------------------
# version
# ----------------------------------------------------------------


def test_version() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout


# ----------------------------------------------------------------
# init — scaffold a project
# ----------------------------------------------------------------


class TestInit:
    def test_creates_project_structure(self, tmp_path) -> None:
        project = tmp_path / "my-bot"
        result = runner.invoke(app, ["init", str(project)])
        assert result.exit_code == 0
        assert project.exists()
        assert (project / "handlers.py").exists()
        assert (project / "workflows" / "wf_hello.yaml").exists()
        assert (project / "README.md").exists()

    def test_refuses_to_overwrite_without_force(self, tmp_path) -> None:
        existing = tmp_path / "existing"
        existing.mkdir()
        result = runner.invoke(app, ["init", str(existing)])
        assert result.exit_code == 1

    def test_force_overwrite_works(self, tmp_path) -> None:
        existing = tmp_path / "existing"
        existing.mkdir()
        (existing / "handlers.py").write_text("# old", encoding="utf-8")

        result = runner.invoke(app, ["init", str(existing), "--force"])
        assert result.exit_code == 0
        # Newly written content overrides our placeholder.
        assert "@agent" in (existing / "handlers.py").read_text(encoding="utf-8")


# ----------------------------------------------------------------
# validate
# ----------------------------------------------------------------


_VALID_YAML = """\
id: wf_test
version: 1
agents:
  echo:
    role: thinking
    subscribe: [{ topic: q }]
    publish:   [{ topic: r }]
edges:
  e_in:  { from: __start__, to: echo, via: q }
  e_out: { from: echo, to: __end__, via: r }
"""

_INVALID_YAML = """\
id: wf_bad
version: 1
agents:
  ghost:
    role: thinking
edges:
  e_in: { from: __start__, to: nonexistent, via: x }
"""


class TestValidate:
    def test_valid_yaml_succeeds(self, tmp_path) -> None:
        f = tmp_path / "wf.yaml"
        f.write_text(_VALID_YAML, encoding="utf-8")
        result = runner.invoke(app, ["validate", str(f)])
        assert result.exit_code == 0
        assert "wf_test" in result.stdout

    def test_invalid_yaml_fails(self, tmp_path) -> None:
        f = tmp_path / "bad.yaml"
        f.write_text(_INVALID_YAML, encoding="utf-8")
        result = runner.invoke(app, ["validate", str(f)])
        assert result.exit_code == 1


# ----------------------------------------------------------------
# compile
# ----------------------------------------------------------------


class TestCompile:
    def test_compile_to_stdout(self, tmp_path) -> None:
        f = tmp_path / "wf.yaml"
        f.write_text(_VALID_YAML, encoding="utf-8")
        result = runner.invoke(app, ["compile", str(f)])
        assert result.exit_code == 0
        # stdout should be JSON-parseable
        ir = json.loads(result.stdout)
        assert ir["id"] == "wf_test"

    def test_compile_to_file(self, tmp_path) -> None:
        f = tmp_path / "wf.yaml"
        f.write_text(_VALID_YAML, encoding="utf-8")
        out = tmp_path / "ir.json"
        result = runner.invoke(app, ["compile", str(f), "-o", str(out)])
        assert result.exit_code == 0
        ir = json.loads(out.read_text(encoding="utf-8"))
        assert ir["id"] == "wf_test"


# ----------------------------------------------------------------
# schema export
# ----------------------------------------------------------------


class TestSchemaExport:
    def test_schema_to_stdout(self) -> None:
        result = runner.invoke(app, ["schema", "export"])
        assert result.exit_code == 0
        schema = json.loads(result.stdout)
        # Top-level keys typical of Pydantic-generated JSON Schema.
        assert "$defs" in schema or "properties" in schema

    def test_schema_to_file(self, tmp_path) -> None:
        out = tmp_path / "schema.json"
        result = runner.invoke(app, ["schema", "export", "-o", str(out)])
        assert result.exit_code == 0
        schema = json.loads(out.read_text(encoding="utf-8"))
        assert isinstance(schema, dict)


# ----------------------------------------------------------------
# run — using the scaffold from `init`, sanity check end-to-end
# ----------------------------------------------------------------


class TestRunCmd:
    def test_init_then_run(self, tmp_path, monkeypatch) -> None:
        project = tmp_path / "smoke"
        runner.invoke(app, ["init", str(project)])

        # Switch CWD so handlers.py is importable as `handlers`.
        monkeypatch.chdir(project)
        # Clear handler registry to avoid pollution from earlier tests.
        from agentkit.runtime import HandlerRegistry
        HandlerRegistry.global_default().clear()

        result = runner.invoke(
            app,
            [
                "run",
                "workflows/wf_hello.yaml",
                "--input", '{"q": "ping"}',
                "--handlers", "handlers",
                "--timeout", "5",
            ],
        )
        assert result.exit_code == 0, result.stdout
        assert "run.Succeeded" in result.stdout
        # Final event payload should echo back the input.
        assert "ping" in result.stdout
