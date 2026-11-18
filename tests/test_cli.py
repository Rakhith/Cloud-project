"""Unit tests for the stdlib-only CLI helpers."""

from __future__ import annotations

import argparse
import hashlib
from email.message import Message

import objstore_cli as cli


def test_download_checksum_header_is_case_insensitive(tmp_path, monkeypatch, capsys):
    data = b"download body"
    headers = Message()
    headers["x-checksum-sha256"] = hashlib.sha256(data).hexdigest()
    headers["content-type"] = "text/plain"

    class Response:
        def __init__(self):
            self.body = data
            self.headers = headers

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self, _size):
            body, self.body = self.body, b""
            return body

    client = cli.Client("http://example.test")
    monkeypatch.setattr(client, "_open", lambda *_args, **_kwargs: Response())
    output = tmp_path / "download.bin"

    cli.cmd_download(client, argparse.Namespace(output=str(output), object_id="object-id"))

    assert output.read_bytes() == data
    assert "OK" in capsys.readouterr().out


def test_parse_kv_types_values():
    assert cli.parse_kv(["a=1", "b=true", "c=hello", "d=x=y", "e=1.5"]) == {
        "a": 1, "b": True, "c": "hello", "d": "x=y", "e": 1.5}


def test_human_sizes():
    assert cli.human(512) == "512 B"
    assert cli.human(1024 * 1024) == "1.00 MB"


def test_table_alignment():
    out = cli.table([["a", 1], ["bbb", 22]], ["NAME", "N"]).splitlines()
    assert out[0].startswith("NAME") and set(out[1]) <= {"-", " "} and len(out) == 4
