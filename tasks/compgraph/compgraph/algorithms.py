from __future__ import annotations

import math
import typing as tp
from datetime import datetime
from itertools import chain
import calendar

from . import Graph, operations


def word_count_graph(input_stream_name: str, text_column: str = 'text', count_column: str = 'count') -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""
    return Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])


def inverted_index_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
                         result_column: str = 'tf_idf') -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair"""

    class IDFMapper(operations.Mapper):
        def __init__(self, word_column: str, docs_with_word_col: str, total_docs_col: str, result: str) -> None:
            self._word_column = word_column
            self._docs_with_word_col = docs_with_word_col
            self._total_docs_col = total_docs_col
            self._result = result

        def __call__(self, row: operations.TRow) -> operations.TRowsGenerator:
            docs_with_word = row[self._docs_with_word_col]
            total_docs = row[self._total_docs_col]
            idf_value = math.log(total_docs / docs_with_word) if docs_with_word else 0
            new_row = dict(row)
            new_row[self._result] = idf_value
            yield new_row

    class TfIdfMapper(operations.Mapper):
        def __init__(self, tf_column: str, idf_column: str, result: str) -> None:
            self._tf_column = tf_column
            self._idf_column = idf_column
            self._result = result

        def __call__(self, row: operations.TRow) -> operations.TRowsGenerator:
            tf_val = row[self._tf_column]
            idf_val = row[self._idf_column]
            new_row = dict(row)
            new_row[self._result] = tf_val * idf_val
            yield new_row

    # prepare words
    split_words = Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column))

    # total documents count
    count_docs = Graph.graph_from_iter(input_stream_name) \
        .reduce(operations.Count('doc_count'), [])

    # idf part
    idf_graph = split_words \
        .sort([doc_column, text_column]) \
        .reduce(operations.FirstReducer(), [doc_column, text_column]) \
        .sort([text_column]) \
        .reduce(operations.Count('docs_with_word'), [text_column]) \
        .join(operations.InnerJoiner(), count_docs, []) \
        .map(IDFMapper(text_column, 'docs_with_word', 'doc_count', 'idf'))

    # tf part
    tf_graph = split_words \
        .sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column, 'tf'), [doc_column]) \
        .sort([text_column])

    # combine
    return tf_graph \
        .join(operations.InnerJoiner(), idf_graph, [text_column]) \
        .map(TfIdfMapper('tf', 'idf', result_column)) \
        .sort([text_column]) \
        .reduce(operations.TopN(result_column, 3), [text_column]) \
        .map(operations.Project([doc_column, text_column, result_column]))


def pmi_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
              result_column: str = 'pmi') -> Graph:
    """Constructs graph which gives for every document the top 10 words ranked by pointwise mutual information"""

    class PmiMapper(operations.Mapper):
        def __init__(self, num_col: str, denom_col: str, result: str) -> None:
            self._num_col = num_col
            self._denom_col = denom_col
            self._result = result

        def __call__(self, row: operations.TRow) -> operations.TRowsGenerator:
            numerator = row[self._num_col]
            denominator = row[self._denom_col]
            new_row = dict(row)
            new_row[self._result] = math.log(numerator / denominator)
            yield new_row

    class RatioMapper(operations.Mapper):
        def __init__(self, num_col: str, denom_col: str, result: str) -> None:
            self._num_col = num_col
            self._denom_col = denom_col
            self._result = result

        def __call__(self, row: operations.TRow) -> operations.TRowsGenerator:
            numerator = row[self._num_col]
            denominator = row[self._denom_col]
            new_row = dict(row)
            new_row[self._result] = numerator / denominator if denominator else 0
            yield new_row

    base_words = Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column))

    doc_counts = base_words \
        .sort([doc_column, text_column]) \
        .reduce(operations.Count('doc_count'), [doc_column, text_column]) \
        .map(operations.Filter(lambda row: len(row[text_column]) > 4 and row['doc_count'] >= 2))

    doc_lengths = doc_counts \
        .sort([doc_column]) \
        .reduce(operations.Sum('doc_count'), [doc_column]) \
        .map(operations.Project([doc_column, 'doc_count']))

    global_counts = doc_counts \
        .sort([text_column]) \
        .reduce(operations.Sum('doc_count'), [text_column])

    total_words = doc_counts.reduce(operations.Sum('doc_count'), [])

    global_freq = global_counts \
        .join(operations.InnerJoiner(), total_words, []) \
        .map(RatioMapper('doc_count_1', 'doc_count_2', 'global_freq')) \
        .map(operations.Project([text_column, 'global_freq'])) \
        .sort([text_column])

    doc_freq = doc_counts \
        .join(operations.InnerJoiner(), doc_lengths, [doc_column]) \
        .map(RatioMapper('doc_count_1', 'doc_count_2', 'doc_freq')) \
        .sort([text_column, doc_column])

    return doc_freq \
        .join(operations.InnerJoiner(), global_freq, [text_column]) \
        .map(PmiMapper('doc_freq', 'global_freq', result_column)) \
        .map(operations.Project([doc_column, text_column, result_column])) \
        .sort([doc_column]) \
        .reduce(operations.TopN(result_column, 10), [doc_column])


def yandex_maps_graph(
    input_stream_name_time: str,
    input_stream_name_length: str,
    enter_time_column: str = "enter_time",
    leave_time_column: str = "leave_time",
    edge_id_column: str = "edge_id",
    start_coord_column: str = "start",
    end_coord_column: str = "end",
    weekday_result_column: str = "weekday",
    hour_result_column: str = "hour",
    speed_result_column: str = "speed",
) -> Graph:
    import datetime
    import math

    def _parse_dt(val: str) -> datetime.datetime:
        if not val:
            return None
        try:
            return datetime.datetime.strptime(val, "%Y%m%dT%H%M%S.%f")
        except Exception:
            return None

    def _weekday(row: dict):
        dt = _parse_dt(row.get(enter_time_column))
        return dt.strftime("%a") if dt else None

    def _hour(row: dict):
        dt = _parse_dt(row.get(enter_time_column))
        return dt.hour if dt else None

    def _duration(row: dict):
        enter = _parse_dt(row.get(enter_time_column))
        leave = _parse_dt(row.get(leave_time_column))
        if not enter or not leave or leave <= enter:
            return 0.0
        return (leave - enter).total_seconds() / 3600.0  # часы

    # ---------------- Время граф ----------------
    time_graph = (
        Graph.graph_from_iter(input_stream_name_time)
        .map(operations.ComputeColumn(weekday_result_column, _weekday))
        .map(operations.ComputeColumn(hour_result_column, _hour))
        .map(operations.ComputeColumn("duration", _duration))
        .map(
            operations.Project(
                [weekday_result_column, hour_result_column, edge_id_column, "duration"]
            )
        )
        .map(operations.Filter(condition=lambda r: r.get("duration", 0) > 0))
    )

    # ---------------- Длина граф ----------------
    def _haversine_km(a, b):
        if not a or not b:
            return 0.0
        lon1, lat1 = map(math.radians, a)
        lon2, lat2 = map(math.radians, b)
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        R = 6371000.0
        hav = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        return 2 * R * math.asin(math.sqrt(hav)) / 1000.0

    def _compute_length(row: dict) -> float:
        val = row.get("length")
        if val is not None:
            try:
                f = float(val)
                return f / 1000.0 if f > 100 else f
            except Exception:
                pass
        return _haversine_km(row.get(start_coord_column), row.get(end_coord_column))

    length_graph = (
        Graph.graph_from_iter(input_stream_name_length)
        .map(operations.ComputeColumn("length_km", _compute_length))
        .map(operations.Filter(condition=lambda r: r.get("length_km", 0) > 0))
        .map(operations.Project([edge_id_column, "length_km"]))
    )

    # ---------------- Join time_graph и length_graph ----------------
    joined_graph = time_graph.join(
        operations.InnerJoiner(), length_graph, keys=[edge_id_column]
    )

    # ---------------- Вычисление скорости ----------------
    def _speed(row: dict):
        dur = row.get("duration", 0)
        length = row.get("length_km", 0)
        if not dur or dur <= 0:
            return None
        return length / dur

    joined_graph = joined_graph.map(
        operations.ComputeColumn(speed_result_column, _speed)
    ).map(
        operations.Filter(
            condition=lambda r: r.get(speed_result_column) is not None
            and r.get(speed_result_column) > 0
        )
    )

    # ---------------- Агрегация: средняя скорость по weekday + hour ----------------
    agg_graph = joined_graph.reduce(
        operations.Average("speed", "speed"),
        [
            weekday_result_column,
            hour_result_column,
        ],
    )

    return agg_graph
