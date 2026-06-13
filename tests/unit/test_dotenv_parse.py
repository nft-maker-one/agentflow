"""`.env` value parsing — inline comments, quotes, whitespace.

Regression guard for the loader bug where ``KEY=value   # comment`` kept
the trailing comment in the value (a 46-char Telegram token loaded as
107 chars → auth failures).
"""

from __future__ import annotations

import pytest

from agentkit.api.server import _autoload_dotenv, _parse_dotenv_value


@pytest.mark.parametrize(
    "raw,expected",
    [
        # inline comment after an unquoted value is dropped
        ("8973653471:AAExmx-ubuHKQnjFE   # @BotFather note",
         "8973653471:AAExmx-ubuHKQnjFE"),
        ("465  # SSL port", "465"),
        # quoted value preserves inner '#' and spaces; trailing comment dropped
        ('"value # keep"  # cmt', "value # keep"),
        ("'single quoted'", "single quoted"),
        # '#' NOT preceded by whitespace stays in the value
        ("a#b", "a#b"),
        # plain + surrounding whitespace
        ("plain", "plain"),
        ("  spaced  ", "spaced"),
        # empty after comment
        ("   # only a comment", ""),
    ],
)
def test_parse_dotenv_value(raw: str, expected: str) -> None:
    assert _parse_dotenv_value(raw) == expected


def test_autoload_strips_inline_comment(tmp_path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "MYTOKEN=123:ABC-def   # inline comment here\n"
        "MYPORT=465 # ssl\n"
        'MYQUOTED="has # hash"\n'
        "# a full-line comment\n"
        "export MYEXPORT=val   # with export prefix\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    for k in ("MYTOKEN", "MYPORT", "MYQUOTED", "MYEXPORT"):
        monkeypatch.delenv(k, raising=False)

    _autoload_dotenv()

    import os
    assert os.environ["MYTOKEN"] == "123:ABC-def"
    assert os.environ["MYPORT"] == "465"
    assert os.environ["MYQUOTED"] == "has # hash"
    assert os.environ["MYEXPORT"] == "val"
