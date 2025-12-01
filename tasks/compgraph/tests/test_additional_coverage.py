from pathlib import Path

from compgraph import Graph, operations


def test_graph_from_file_reads_lines(tmp_path: Path) -> None:
    source = tmp_path / "data.txt"
    source.write_text("1;hello\n2;world\n")

    def parser(line: str) -> operations.TRow:
        doc_id, text = line.strip().split(";")
        return {"doc_id": int(doc_id), "text": text}

    result = Graph.graph_from_file(str(source), parser).run()

    assert list(result) == [
        {"doc_id": 1, "text": "hello"},
        {"doc_id": 2, "text": "world"},
    ]


def test_compute_column_maps_function() -> None:
    def add_length(row: operations.TRow) -> int:
        return len(row["text"])

    rows = [
        {"text": "Hi"},
        {"text": "longer"},
    ]

    graph = Graph.graph_from_iter("rows").map(
        operations.ComputeColumn("text_length", add_length)
    )

    assert list(graph.run(rows=lambda: iter(rows))) == [
        {"text": "Hi", "text_length": 2},
        {"text": "longer", "text_length": 6},
    ]


def test_average_handles_invalid_values() -> None:
    reducer = operations.Average("value", result_column="avg")
    data = [
        {"group": "a", "value": 2},
        {"group": "a", "value": 4},
        {"group": "b", "value": None},
        {"group": "b", "value": "oops"},
        {"group": "b", "value": 6},
    ]

    graph = Graph.graph_from_iter("rows").reduce(reducer, ["group"])

    assert list(graph.run(rows=lambda: iter(data))) == [
        {"group": "a", "avg": 3.0},
        {"group": "b", "avg": 6.0},
    ]


def test_average_skips_groups_without_numeric_values() -> None:
    reducer = operations.Average("value")
    rows = [
        {"group": "x", "value": None},
        {"group": "x", "value": "nope"},
    ]

    graph = Graph.graph_from_iter("rows").reduce(reducer, ["group"])

    assert list(graph.run(rows=lambda: iter(rows))) == []
