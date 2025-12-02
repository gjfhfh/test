from pathlib import Path

from compgraph import Graph, operations


def materialize(gen):
    return list(gen)


def test_graph_from_file_reads_and_maps_rows(tmp_path: Path):
    input_path = tmp_path / "numbers.txt"
    input_path.write_text("1\n2\n3\n")

    graph = Graph.graph_from_file(
        str(input_path),
        parser=lambda line: {"value": int(line.strip())},
    ).map(operations.ComputeColumn("double", lambda row: row["value"] * 2))

    assert materialize(graph.run()) == [
        {"value": 1, "double": 2},
        {"value": 2, "double": 4},
        {"value": 3, "double": 6},
    ]


def test_graph_from_iter_can_sort_and_reduce(tmp_path: Path):
    graph = (
        Graph.graph_from_iter("rows")
        .sort(["group"])
        .reduce(operations.Sum("value"), ["group"])
    )

    rows = [
        {"group": "a", "value": 1},
        {"group": "b", "value": 2},
        {"group": "a", "value": 3},
    ]

    assert materialize(graph.run(rows=lambda: iter(rows))) == [
        {"group": "a", "value": 4},
        {"group": "b", "value": 2},
    ]
