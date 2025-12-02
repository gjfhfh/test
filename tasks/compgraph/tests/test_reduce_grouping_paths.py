
from compgraph.operations import Reduce, FirstReducer, Count


def materialize(gen):
    return list(gen)


def test_reduce_handles_empty_iterator():
    reducer = Reduce(Count("cnt"), ["key"])
    assert materialize(reducer(iter([]))) == []


def test_reduce_drains_group_iterator_when_reducer_stops_early():
    reducer = Reduce(FirstReducer(), ["k"])
    rows = [
        {"k": 1, "v": "a"},
        {"k": 1, "v": "b"},
        {"k": 2, "v": "c"},
        {"k": 2, "v": "d"},
    ]
    result = materialize(reducer(iter(rows)))
    assert result == [
        {"k": 1, "v": "a"},
        {"k": 2, "v": "c"},
    ]


def test_reduce_preserves_multiple_groups_with_single_row_each():
    reducer = Reduce(Count("cnt"), ["k"])
    rows = [
        {"k": 1, "v": 10},
        {"k": 2, "v": 20},
        {"k": 3, "v": 30},
    ]
    assert materialize(reducer(iter(rows))) == [
        {"k": 1, "cnt": 1},
        {"k": 2, "cnt": 1},
        {"k": 3, "cnt": 1},
    ]
