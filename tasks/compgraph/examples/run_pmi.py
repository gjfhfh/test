"""Compute per-document PMI top words for a JSONL corpus."""
from __future__ import annotations

import argparse

from compgraph.algorithms import pmi_graph
from examples.utils import read_json_lines, write_json_lines


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Path to JSONL file with rows containing 'doc_id' and 'text'")
    parser.add_argument(
        "--output",
        default="-",
        help="Where to store results (JSONL). Use '-' to print to stdout.",
    )
    args = parser.parse_args(argv)

    graph = pmi_graph(input_stream_name="docs")
    rows = graph.run(docs=lambda: read_json_lines(args.input))
    write_json_lines(rows, args.output)


if __name__ == "__main__":
    main()
