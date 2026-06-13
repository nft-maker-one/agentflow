"""Unit tests for the YAML loader."""

from __future__ import annotations

from pathlib import Path

import pytest

from agentkit.workflow.errors import CompileError
from agentkit.workflow.yaml_loader import load_workflow_yaml


def _write(tmp_path: Path, name: str, body: str) -> Path:
    p = tmp_path / name
    p.write_text(body, encoding="utf-8")
    return p


def test_loads_with_workflow_wrapper(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "wf.yaml",
        """
workflow:
  id: wf_test
  version: 1
  agents:
    a:
      role: thinking
""",
    )
    raw = load_workflow_yaml(p)
    assert raw["id"] == "wf_test"


def test_loads_flat_document(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "wf.yaml",
        """
id: wf_flat
version: 1
agents: {}
""",
    )
    raw = load_workflow_yaml(p)
    assert raw["id"] == "wf_flat"


def test_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(CompileError, match="not found"):
        load_workflow_yaml(tmp_path / "nope.yaml")


def test_invalid_yaml_raises(tmp_path: Path) -> None:
    p = _write(tmp_path, "bad.yaml", "id: [unterminated")
    with pytest.raises(CompileError, match="parse error"):
        load_workflow_yaml(p)


def test_empty_yaml_rejected(tmp_path: Path) -> None:
    p = _write(tmp_path, "empty.yaml", "")
    with pytest.raises(CompileError, match="empty"):
        load_workflow_yaml(p)


def test_root_must_be_mapping(tmp_path: Path) -> None:
    p = _write(tmp_path, "list.yaml", "- foo\n- bar\n")
    with pytest.raises(CompileError, match="mapping"):
        load_workflow_yaml(p)
