import math

from compgraph.operations import Average, Count, Sum, TermFrequency, TopN, Reduce, Map, ComputeColumn


def materialize(gen):
    return list(gen)


def test_count_and_sum_on_empty_iterable():
    count_reducer = Reduce(Count("cnt"), ["key"])
    sum_reducer = Reduce(Sum("value"), ["key"])

    assert materialize(count_reducer(iter([]))) == []
    assert materialize(sum_reducer(iter([]))) == []


def test_average_skips_invalid_and_none():
    reducer = Reduce(Average("value", "avg"), ["group"])
    rows = [
        {"group": 1, "value": None},
        {"group": 1, "value": "not-a-number"},
        {"group": 1, "value": 4},
        {"group": 1, "value": 6},
    ]
    assert materialize(reducer(iter(rows))) == [{"group": 1, "avg": 5.0}]


def test_term_frequency_empty_and_single():
    empty_tf = Reduce(TermFrequency("word", "tf"), ["doc"])
    assert materialize(empty_tf(iter([]))) == []

    single_tf = Reduce(TermFrequency("word", "tf"), ["doc"])
    rows = [{"doc": 1, "word": "a"}, {"doc": 1, "word": "a"}, {"doc": 1, "word": "b"}]
    assert materialize(single_tf(iter(rows))) == [
        {"doc": 1, "word": "a", "tf": 2 / 3},
        {"doc": 1, "word": "b", "tf": 1 / 3},
    ]


def test_topn_replaces_and_keeps_order_for_ties():
    reducer = Reduce(TopN("score", 2), ["group"])
    rows = [
        {"group": 1, "score": 1, "id": "first"},
        {"group": 1, "score": 3, "id": "largest"},
        {"group": 1, "score": 2, "id": "middle"},
        {"group": 1, "score": 3, "id": "latest_top"},
    ]
    result = materialize(reducer(iter(rows)))
    assert result == [
        {"group": 1, "score": 3, "id": "largest"},
        {"group": 1, "score": 3, "id": "latest_top"},
    ]


def test_compute_column_with_map_integration():
    mapper = Map(ComputeColumn("root", lambda r: math.sqrt(r["value"])))
    rows = [{"value": 9}, {"value": 16}]
    assert materialize(mapper(iter(rows))) == [
        {"value": 9, "root": 3.0},
        {"value": 16, "root": 4.0},
    ]
