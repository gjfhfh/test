
from compgraph.operations import OuterJoiner, _row_key


def materialize(gen):
    return list(gen)


def test_outer_joiner_emits_left_only_rows():
    joiner = OuterJoiner()
    left = [{"id": 1, "v": "a"}]
    right: list[dict] = []
    assert materialize(joiner(["id"], left, right)) == [{"id": 1, "v": "a"}]


def test_outer_joiner_emits_right_only_rows():
    joiner = OuterJoiner()
    left: list[dict] = []
    right = [{"id": 2, "v": "b"}]
    assert materialize(joiner(["id"], left, right)) == [{"id": 2, "v": "b"}]


def test_outer_joiner_cross_product_for_shared_keys():
    joiner = OuterJoiner()
    left = [
        {"id": 1, "la": "a1"},
        {"id": 1, "la": "a2"},
    ]
    right = [
        {"id": 1, "rb": "b1"},
        {"id": 1, "rb": "b2"},
    ]
    result = materialize(joiner(["id"], left, right))
    assert len(result) == 4
    assert {(_row_key(r, ["id"]), r["la"], r["rb"]) for r in result} == {
        ((1,), "a1", "b1"),
        ((1,), "a1", "b2"),
        ((1,), "a2", "b1"),
        ((1,), "a2", "b2"),
    }
