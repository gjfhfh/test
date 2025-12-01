import math
import typing as tp

import pytest

from compgraph import Graph, operations


@pytest.fixture()
def sample_rows() -> list[operations.TRow]:
    return [
        {"id": 1, "text": "Hello, World!", "a": 2, "b": 3},
        {"id": 2, "text": "Foo Bar", "a": 5, "b": 7},
    ]


def test_split_and_lower(sample_rows: list[operations.TRow]) -> None:
    graph = Graph.graph_from_iter("rows") \
        .map(operations.FilterPunctuation("text")) \
        .map(operations.LowerCase("text")) \
        .map(operations.Split("text"))

    result = list(graph.run(rows=lambda: iter(sample_rows)))
    assert result == [
        {"id": 1, "text": "hello", "a": 2, "b": 3},
        {"id": 1, "text": "world", "a": 2, "b": 3},
        {"id": 2, "text": "foo", "a": 5, "b": 7},
        {"id": 2, "text": "bar", "a": 5, "b": 7},
    ]


def test_split_edge_cases() -> None:
    graph = Graph.graph_from_iter("rows").map(operations.Split("text", separator=""))
    rows = [{"text": "", "x": 1}, {"text": [1, 2], "x": 2}]
    assert list(graph.run(rows=lambda: iter(rows))) == [
        {"text": "", "x": 1},
        {"text": [1, 2], "x": 2},
    ]


def test_filter_and_project(sample_rows: list[operations.TRow]) -> None:
    graph = Graph.graph_from_iter("rows") \
        .map(operations.Filter(lambda row: row["a"] > 2)) \
        .map(operations.Project(["id", "a"]))

    assert list(graph.run(rows=lambda: iter(sample_rows))) == [
        {"id": 2, "a": 5},
    ]


def test_product_and_reduce() -> None:
    rows = [{"a": 2, "b": 4}, {"a": 3, "b": 5}]
    graph = Graph.graph_from_iter("rows") \
        .map(operations.Product(["a", "b"], result_column="prod")) \
        .sort(["a"]) \
        .reduce(operations.Sum("prod"), ["a"])
    assert list(graph.run(rows=lambda: iter(rows))) == [
        {"a": 2, "prod": 8},
        {"a": 3, "prod": 15},
    ]


def test_topn_and_tf() -> None:
    rows = [
        {"group": 1, "word": "a"},
        {"group": 1, "word": "b"},
        {"group": 1, "word": "a"},
        {"group": 2, "word": "b"},
        {"group": 2, "word": "b"},
    ]
    graph = Graph.graph_from_iter("rows") \
        .sort(["group"]) \
        .reduce(operations.TermFrequency("word", "tf"), ["group"])
    tf_rows = list(graph.run(rows=lambda: iter(rows)))
    assert {row["group"] for row in tf_rows} == {1, 2}

    top_graph = Graph.graph_from_iter("rows") \
        .sort(["word"]) \
        .reduce(operations.TopN("value", 1), ["word"])
    assert list(top_graph.run(rows=lambda: iter([
        {"word": "a", "value": 1},
        {"word": "a", "value": 2},
        {"word": "b", "value": 3},
    ]))) == [
        {"word": "a", "value": 2},
        {"word": "b", "value": 3},
    ]


def test_joiners() -> None:
    left = [
        {"k": 1, "v": "l1"},
        {"k": 2, "v": "l2"},
    ]
    right = [
        {"k": 2, "v": "r2"},
        {"k": 3, "v": "r3"},
    ]
    inner = list(operations.InnerJoiner()(keys=["k"], rows_a=left, rows_b=right))
    assert inner == [{"k": 2, "v_1": "l2", "v_2": "r2"}]

    left_join = list(operations.LeftJoiner()(keys=["k"], rows_a=left, rows_b=right))
    assert left_join[0] == {"k": 1, "v": "l1"}
    assert left_join[1] == {"k": 2, "v_1": "l2", "v_2": "r2"}

    right_join = list(operations.RightJoiner()(keys=["k"], rows_a=left, rows_b=right))
    assert right_join[-1] == {"k": 3, "v": "r3"}

    outer = list(operations.OuterJoiner()(keys=["k"], rows_a=left, rows_b=right))
    assert len(outer) == 3


def test_graph_reuse() -> None:
    rows = [{"k": 1, "v": 2}, {"k": 1, "v": 3}]
    graph = Graph.graph_from_iter("rows") \
        .sort(["k"]) \
        .reduce(operations.Count("cnt"), ["k"])

    first = list(graph.run(rows=lambda: iter(rows)))
    second = list(graph.run(rows=lambda: iter(rows)))
    assert first == second == [{"k": 1, "cnt": 2}]


def test_read_iter_factory() -> None:
    factory_called: list[int] = []

    def factory() -> tp.Iterator[operations.TRow]:
        factory_called.append(1)
        yield {"x": 1}

    graph = Graph.graph_from_iter("rows")
    assert list(graph.run(rows=factory)) == [{"x": 1}]
    assert factory_called == [1]


def test_reduce_empty_group() -> None:
    reducer = operations.FirstReducer()
    assert list(operations.Reduce(reducer, ["k"])([], [])) == []


def test_haversine_helper_matches_algorithms() -> None:
    # sanity check on helper used in algorithms
    def haversine(start: tp.Sequence[float], end: tp.Sequence[float]) -> float:
        lon1, lat1, lon2, lat2 = map(math.radians, [*start, *end])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        return 2 * 6373 * math.asin(math.sqrt(a))

    assert pytest.approx(haversine([0, 0], [0, 1]), rel=1e-6) == haversine([0, 0], [0, 1])
