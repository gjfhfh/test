"""Calculate average city speed by weekday and hour."""
from __future__ import annotations

import argparse

from compgraph.algorithms import yandex_maps_graph
from examples.utils import read_json_lines, write_json_lines


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--travel-times", required=True, help="JSONL with enter/leave times and edge_id")
    parser.add_argument("--edges", required=True, help="JSONL with edge_id metadata (length/start/end)")
    parser.add_argument(
        "--output",
        default="-",
        help="Where to store results (JSONL). Use '-' to print to stdout.",
    )
    args = parser.parse_args(argv)

    graph = yandex_maps_graph(
        input_stream_name_time="times",
        input_stream_name_length="edges",
    )
    rows = graph.run(
        times=lambda: read_json_lines(args.travel_times),
        edges=lambda: read_json_lines(args.edges),
    )
    write_json_lines(rows, args.output)


if __name__ == "__main__":
    main()
