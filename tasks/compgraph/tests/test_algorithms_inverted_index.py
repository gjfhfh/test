import pytest

from compgraph import algorithms


def materialize(gen):
    return list(gen)


def test_inverted_index_tfidf_top_results():
    docs = [
        {"doc_id": "doc1", "text": "Apple apple apple"},
        {"doc_id": "doc2", "text": "Apple banana"},
    ]

    graph = algorithms.inverted_index_graph("docs")

    result = materialize(graph.run(docs=lambda: iter(docs)))

    assert result == [
        {"doc_id": "doc1", "text": "apple", "tf_idf": 0.0},
        {"doc_id": "doc2", "text": "apple", "tf_idf": 0.0},
        {"doc_id": "doc2", "text": "banana", "tf_idf": pytest.approx(0.3465, rel=1e-3)},
    ]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
