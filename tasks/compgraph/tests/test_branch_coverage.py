from compgraph import operations


def test_reduce_handles_empty_iterable():
    class PassthroughReducer(operations.Reducer):
        def __call__(
            self, group_key: tuple[str, ...], rows: operations.TRowsIterable
        ) -> operations.TRowsGenerator:
            yield from rows

    reducer = operations.Reduce(PassthroughReducer(), keys=["k"])
    assert list(reducer(iter([]))) == []


def test_split_non_string_and_empty_separator():
    split = operations.Split("value")
    assert list(split({"value": 10})) == [{"value": 10}]

    empty_split = operations.Split("text", separator="")
    assert list(empty_split({"text": "ab"})) == [
        {"text": "a"},
        {"text": "b"},
    ]
    assert list(empty_split({"text": ""})) == [{"text": ""}]


def test_term_frequency_and_reducers_handle_no_rows():
    tf = operations.TermFrequency("word", "tf")
    assert list(tf(("word",), iter([]))) == []

    cnt = operations.Count("amount")
    assert list(cnt(("key",), iter([]))) == []

    total = operations.Sum("amount")
    assert list(total(("key",), iter([]))) == []


def test_inner_joiner_early_terminations():
    joiner = operations.InnerJoiner()
    assert list(joiner(["k"], iter([]), iter([{"k": 1}]))) == []
    assert list(joiner(["k"], iter([{"k": 1}]), iter([]))) == []
    assert list(joiner(["k"], iter([{"k": 1}]), iter([{"k": 2}]))) == []


def test_left_joiner_edge_paths():
    joiner = operations.LeftJoiner()

    assert list(joiner(["k"], iter([]), iter([{"k": 1}]))) == []

    assert list(joiner(["k"], iter([{"k": 1}, {"k": 2}]), iter([]))) == [
        {"k": 1},
        {"k": 2},
    ]

    assert list(
        joiner(
            ["k"],
            iter([{"k": 1}, {"k": 1}, {"k": 2}]),
            iter([{"k": 2}]),
        )
    ) == [
        {"k": 1},
        {"k": 1},
        {"k": 2},
    ]

    assert list(joiner(["k"], iter([{"k": 2}]), iter([{"k": 1}]))) == [
        {"k": 2}
    ]

    assert list(
        joiner(
            ["k"],
            iter([{"k": 2}]),
            iter([{"k": 1}, {"k": 1}, {"k": 2}]),
        )
    ) == [{"k": 2}]

    assert list(
        joiner(
            ["k"],
            iter([{"k": 1}, {"k": 2}]),
            iter([{"k": 1}]),
        )
    ) == [
        {"k": 1},
        {"k": 2},
    ]


def test_right_joiner_edge_paths():
    joiner = operations.RightJoiner()

    assert list(joiner(["k"], iter([]), iter([{"k": 1}, {"k": 2}]))) == [
        {"k": 1},
        {"k": 2},
    ]

    assert list(joiner(["k"], iter([{"k": 1}]), iter([]))) == []

    assert list(joiner(["k"], iter([{"k": 1}, {"k": 3}]), iter([{"k": 2}]))) == [
        {"k": 2}
    ]

    assert list(joiner(["k"], iter([{"k": 2}]), iter([{"k": 1}, {"k": 1}]))) == [
        {"k": 1},
        {"k": 1},
    ]

