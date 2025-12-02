from __future__ import annotations

import json
from pathlib import Path

from examples import run_word_count, run_yandex_maps
from psutil import Process


def _read_output(path: Path) -> list[dict]:
    content = path.read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in content]


def test_psutil_stub_memory_info() -> None:
    info = Process().memory_info()
    assert hasattr(info, "rss")
    assert isinstance(info.rss, int)
    assert info.rss >= 0


def test_word_count_example_cli(tmp_path: Path) -> None:
    input_path = tmp_path / "docs.jsonl"
    input_rows = [
        {"doc_id": 1, "text": "Hello world"},
        {"doc_id": 2, "text": "Hello there"},
    ]
    input_path.write_text("\n".join(json.dumps(row) for row in input_rows), encoding="utf-8")

    output_path = tmp_path / "out.jsonl"
    run_word_count.main(["--input", str(input_path), "--output", str(output_path)])

    rows = _read_output(output_path)
    assert rows == [
        {"text": "there", "count": 1},
        {"text": "world", "count": 1},
        {"text": "hello", "count": 2},
    ]


def test_yandex_maps_example_cli(tmp_path: Path) -> None:
    travel_path = tmp_path / "travel.jsonl"
    travel_row = {
        "edge_id": "1",
        "enter_time": "20170912T010000.000000",
        "leave_time": "20170912T020000.000000",
    }
    travel_path.write_text(json.dumps(travel_row), encoding="utf-8")

    edges_path = tmp_path / "edges.jsonl"
    edges_row = {
        "edge_id": "1",
        "length": 1000,
        "start": [37.0, 55.0],
        "end": [37.01, 55.01],
    }
    edges_path.write_text(json.dumps(edges_row), encoding="utf-8")

    output_path = tmp_path / "speeds.jsonl"
    run_yandex_maps.main(
        [
            "--travel-times",
            str(travel_path),
            "--edges",
            str(edges_path),
            "--output",
            str(output_path),
        ]
    )

    rows = _read_output(output_path)
    assert rows == [{"weekday": "Tue", "hour": 1, "speed": 1.0}]
