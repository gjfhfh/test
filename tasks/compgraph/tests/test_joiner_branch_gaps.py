from compgraph.operations import LeftJoiner, RightJoiner


def materialize(gen):
    return list(gen)


def test_left_joiner_skips_right_prefix_until_keys_align():
    joiner = LeftJoiner()
    rows_a = [
        {"id": 2, "a": "a2"},
    ]
    rows_b = [
        {"id": 1, "b": "b1"},
        {"id": 1, "b": "b1-dup"},
        {"id": 2, "b": "b2"},
    ]

    assert materialize(joiner(["id"], rows_a, rows_b)) == [
        {"id": 2, "a": "a2", "b": "b2"},
    ]


def test_right_joiner_consumes_duplicate_left_keys_before_emitting_right():
    joiner = RightJoiner()
    rows_a = [
        {"id": 1, "a": "a1"},
        {"id": 1, "a": "a1-dup"},
        {"id": 2, "a": "a2"},
    ]
    rows_b = [
        {"id": 3, "b": "b3"},
    ]

    assert materialize(joiner(["id"], rows_a, rows_b)) == rows_b
