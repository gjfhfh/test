import pytest

from compgraph.operations import (
    Average,
    ComputeColumn,
    Count,
    FirstReducer,
    InnerJoiner,
    Join,
    LeftJoiner,
    Map,
    Reduce,
    RightJoiner,
    Split,
    Sum,
    TermFrequency,
)


def materialize(gen):
    return list(gen)


def test_reduce_handles_empty_iterable():
    reducer = Reduce(FirstReducer(), ["k"])
    assert materialize(reducer([])) == []


def test_split_skips_non_string_and_empty_char_separator():
    mapper_plain = Map(Split("value"))
    row = {"value": 123, "other": "x"}
    assert materialize(mapper_plain([row])) == [row]

    mapper_chars = Map(Split("value", separator=""))
    empty_row = {"value": "", "other": 5}
    assert materialize(mapper_chars([empty_row])) == [{"value": "", "other": 5}]


def test_term_frequency_and_simple_reducers_on_empty_groups():
    tf_reducer = Reduce(TermFrequency("word"), ["doc"])
    count_reducer = Reduce(Count("count"), ["doc"])
    sum_reducer = Reduce(Sum("total"), ["doc"])

    assert materialize(tf_reducer([])) == []
    assert materialize(count_reducer([])) == []
    assert materialize(sum_reducer([])) == []


def test_inner_joiner_empty_sources_and_left_shorter():
    joiner = InnerJoiner()
    assert materialize(joiner(["k"], [], [{"k": 1, "b": 2}])) == []
    assert materialize(joiner(["k"], [{"k": 1, "a": 1}], [])) == []

    # left side ends before any key matches
    assert materialize(joiner(["k"], [{"k": 1, "a": 1}], [{"k": 2, "b": 2}])) == []


def test_left_joiner_covers_all_exits():
    joiner = LeftJoiner()

    # no right rows
    left_only = [{"k": 1}, {"k": 2}]
    assert materialize(joiner(["k"], iter(left_only), iter([]))) == left_only

    # left key smaller than right key
    left_rows = [{"k": 1}, {"k": 3}]
    right_rows = [{"k": 2}]
    assert materialize(joiner(["k"], iter(left_rows), iter(right_rows))) == left_rows

    # right side exhausted while iterating smaller keys
    repeated_right = [{"k": 1}, {"k": 1}]
    trailing_left = [{"k": 2}, {"k": 3}]
    assert materialize(joiner(["k"], iter(trailing_left), iter(repeated_right))) == [
        {"k": 2},
        {"k": 3},
    ]


def test_right_joiner_covers_all_exits():
    joiner = RightJoiner()

    # no left rows
    assert materialize(joiner(["k"], iter([]), iter([{"k": 1}, {"k": 2}]))) == [
        {"k": 1},
        {"k": 2},
    ]

    # no right rows
    assert materialize(joiner(["k"], iter([{"k": 1}]), iter([]))) == []

    # left stream finishes before first join
    assert materialize(
        joiner(["k"], iter([{"k": 1}]), iter([{"k": 2}, {"k": 3}]))
    ) == [
        {"k": 2},
        {"k": 3},
    ]

    # right stream has smaller key and repeated rows
    assert materialize(
        joiner(["k"], iter([{"k": 2}]), iter([{"k": 1}, {"k": 1}]))
    ) == [
        {"k": 1},
        {"k": 1},
    ]


def test_average_skips_missing_values_and_preserves_keys():
    reducer = Reduce(Average("val", result_column="avg"), ["group"])
    rows = [
        {"group": "g1", "val": None},
        {"group": "g1", "val": "2"},
        {"group": "g1", "val": "bad"},
        {"group": "g1", "val": 4},
    ]
    assert materialize(reducer(rows)) == [
        {"group": "g1", "avg": 3.0},
    ]


def test_join_operation_delegates_to_joiner():
    join_op = Join(LeftJoiner(), ["k"])
    rows = list(
        join_op(
            [{"k": 1, "a": 1}],
            [{"k": 1, "b": 2}],
        )
    )
    assert rows == [{"k": 1, "a": 1, "b": 2}]
