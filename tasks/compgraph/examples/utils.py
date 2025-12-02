"""Utility helpers shared across CLI examples."""
from __future__ import annotations

import json
import sys
from typing import Iterable, Iterator


def read_json_lines(path: str) -> Iterator[dict]:
    """Yield dictionaries from a JSONL file."""

    with open(path, "r", encoding="utf-8") as stream:
        for line in stream:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def write_json_lines(rows: Iterable[dict], path: str) -> None:
    """Write iterable of dictionaries as JSONL to *path* (or stdout)."""

    stream = sys.stdout if path == "-" else open(path, "w", encoding="utf-8")
    try:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False))
            stream.write("\n")
    finally:
        if stream is not sys.stdout:
            stream.close()
