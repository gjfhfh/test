from compgraph.operations import LeftJoiner, RightJoiner


def materialize(gen):
    return list(gen)


def test_left_joiner_returns_early_when_left_keys_before_right():
    joiner = LeftJoiner()
    rows_a = [{"id": 1, "a": "a1"}]
    rows_b = [{"id": 2, "b": "b2"}]

    assert materialize(joiner(["id"], rows_a, rows_b)) == rows_a


def test_left_joiner_emits_remaining_left_when_right_exhausted_mid_group():
    joiner = LeftJoiner()
    rows_a = [
        {"id": 1, "a": "a1"},
        {"id": 1, "a": "a2"},
        {"id": 2, "a": "a3"},
    ]
    rows_b = [{"id": 1, "b": "b1"}]

    assert materialize(joiner(["id"], rows_a, rows_b)) == [
        {"id": 1, "a": "a1", "b": "b1"},
        {"id": 1, "a": "a2", "b": "b1"},
        {"id": 2, "a": "a3"},
    ]


def test_right_joiner_consumes_duplicate_left_keys_before_advance():
    joiner = RightJoiner()
    rows_a = [{"id": 1, "a": "a1"}, {"id": 1, "a": "a2"}]
    rows_b = [{"id": 2, "b": "b2"}]

    assert materialize(joiner(["id"], rows_a, rows_b)) == rows_b
