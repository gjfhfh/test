import math

import pytest

from compgraph import algorithms


def materialize(gen):
    return list(gen)


def test_pmi_graph_filters_and_ratios():
    docs = [
        {"doc_id": "a", "text": "apple apple berry"},
        {"doc_id": "b", "text": "apple berry berry"},
        {"doc_id": "c", "text": "berry berry berry"},
    ]

    graph = algorithms.pmi_graph("docs")
    result = materialize(graph.run(docs=lambda: iter(docs)))

    assert result[0]["text"] == "apple"
    assert result[0]["doc_id"] == "a"
    assert math.isfinite(result[0]["pmi"])

    berry_rows = [row for row in result if row["text"] == "berry"]
    assert {row["doc_id"] for row in berry_rows} == {"b", "c"}
    assert all(row["pmi"] >= 0 for row in berry_rows)


def test_pmi_graph_drops_short_or_sparse_tokens():
    docs = [
        {"doc_id": "a", "text": "tiny words tiny words"},
        {"doc_id": "b", "text": "tiny unique"},
    ]

    result = materialize(algorithms.pmi_graph("docs").run(docs=lambda: iter(docs)))

    assert all(len(row["text"]) > 4 for row in result)
    assert all(row["pmi"] == 0 or math.isfinite(row["pmi"]) for row in result)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
