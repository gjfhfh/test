import pytest

from compgraph import operations


def materialize(gen):
    return list(gen)


def test_left_joiner_returns_when_left_exhausted_early():
    joiner = operations.LeftJoiner()
    rows_a = [{"k": 1, "a": 1}]
    rows_b = [{"k": 2, "b": 2}]

    assert materialize(joiner(["k"], iter(rows_a), iter(rows_b))) == rows_a


def test_left_joiner_emits_remaining_left_when_right_ends_during_group():
    joiner = operations.LeftJoiner()
    rows_a = [
        {"k": 1, "a": "x"},
        {"k": 1, "a": "y"},
        {"k": 2, "a": "z"},
    ]
    rows_b = [
        {"k": 1, "b": 100},
    ]

    result = materialize(joiner(["k"], iter(rows_a), iter(rows_b)))
    assert result[:2] == [
        {"k": 1, "a": "x", "b": 100},
        {"k": 1, "a": "y", "b": 100},
    ]
    assert result[2:] == [{"k": 2, "a": "z"}]


def test_right_joiner_skips_repeated_left_keys_before_first_match():
    joiner = operations.RightJoiner()
    rows_a = [
        {"k": 1, "a": "x"},
        {"k": 1, "a": "y"},
        {"k": 3, "a": "z"},
    ]
    rows_b = [
        {"k": 2, "b": "b"},
    ]

    assert materialize(joiner(["k"], iter(rows_a), iter(rows_b))) == rows_b


def test_right_joiner_stops_when_left_finishes_during_equal_group():
    joiner = operations.RightJoiner()
    rows_a = [
        {"k": 1, "a": "x"},
        {"k": 1, "a": "y"},
    ]
    rows_b = [
        {"k": 1, "b": "p"},
        {"k": 1, "b": "q"},
    ]

    assert materialize(joiner(["k"], iter(rows_a), iter(rows_b))) == [
        {"k": 1, "a": "x", "b": "p"},
        {"k": 1, "a": "x", "b": "q"},
        {"k": 1, "a": "y", "b": "p"},
        {"k": 1, "a": "y", "b": "q"},
    ]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
