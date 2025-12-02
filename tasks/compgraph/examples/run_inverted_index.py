"""Run TF-IDF inverted index calculation on a JSONL corpus."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    # Allow running the script directly without installing the package
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from compgraph.algorithms import inverted_index_graph
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

    graph = inverted_index_graph(input_stream_name="docs")
    rows = graph.run(docs=lambda: read_json_lines(args.input))
    write_json_lines(rows, args.output)


if __name__ == "__main__":
    main()
