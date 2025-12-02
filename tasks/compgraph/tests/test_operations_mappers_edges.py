import pytest

from compgraph import operations


def materialize(gen):
    return list(gen)


def test_split_handles_non_string_and_empty_separator():
    mapper = operations.Split("value")
    non_string_row = {"value": 5, "other": "ok"}
    assert materialize(mapper(non_string_row)) == [non_string_row]

    empty_separator_mapper = operations.Split("value", separator="")
    assert materialize(empty_separator_mapper({"value": ""})) == [{"value": ""}]
    assert materialize(empty_separator_mapper({"value": "ab"})) == [
        {"value": "a"},
        {"value": "b"},
    ]


def test_product_and_filter_branches():
    product_mapper = operations.Product(["x", "y"], result_column="z")
    row = {"x": 2, "y": 3}
    assert materialize(product_mapper(row)) == [{"x": 2, "y": 3, "z": 6}]

    even_filter = operations.Filter(lambda r: r.get("value", 0) % 2 == 0)
    assert materialize(even_filter({"value": 3})) == []
    assert materialize(even_filter({"value": 4})) == [{"value": 4}]


def test_project_keeps_only_selected_columns():
    project = operations.Project(["keep", "other"])
    row = {"keep": 1, "other": 2, "drop": 3}
    assert materialize(project(row)) == [{"keep": 1, "other": 2}]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
