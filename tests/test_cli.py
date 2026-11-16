"""Unit tests for the stdlib-only CLI helpers."""

from __future__ import annotations

import objstore_cli as cli


def test_parse_kv_types_values():
    assert cli.parse_kv(["a=1", "b=true", "c=hello", "d=x=y", "e=1.5"]) == {
        "a": 1, "b": True, "c": "hello", "d": "x=y", "e": 1.5}


def test_human_sizes():
    assert cli.human(512) == "512 B"
    assert cli.human(1024 * 1024) == "1.00 MB"


def test_table_alignment():
    out = cli.table([["a", 1], ["bbb", 22]], ["NAME", "N"]).splitlines()
    assert out[0].startswith("NAME") and set(out[1]) <= {"-", " "} and len(out) == 4
