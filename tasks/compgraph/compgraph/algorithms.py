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


def yandex_maps_graph(input_stream_name_time: str, input_stream_name_length: str,
                      enter_time_column: str = 'enter_time', leave_time_column: str = 'leave_time',
                      edge_id_column: str = 'edge_id', start_coord_column: str = 'start', end_coord_column: str = 'end',
                      weekday_result_column: str = 'weekday', hour_result_column: str = 'hour',
                      speed_result_column: str = 'speed') -> Graph:
    """Constructs graph which measures average speed in km/h depending on the weekday and hour"""

    def haversine_km(start: tp.Sequence[float], end: tp.Sequence[float]) -> float:
        lon1, lat1, lon2, lat2 = map(math.radians, chain(start, end))
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        return 2 * 6373 * math.asin(math.sqrt(a))

    class EnrichLength(operations.Mapper):
        def __init__(self, start_col: str, end_col: str) -> None:
            self._start_col = start_col
            self._end_col = end_col

        def __call__(self, row: operations.TRow) -> operations.TRowsGenerator:
            distance_km = haversine_km(row[self._start_col], row[self._end_col])
            new_row = dict(row)
            new_row['distance_km'] = distance_km
            yield new_row

    class ExtractTime(operations.Mapper):
        def __init__(self, enter_col: str, leave_col: str, weekday_col: str, hour_col: str) -> None:
            self._enter_col = enter_col
            self._leave_col = leave_col
            self._weekday_col = weekday_col
            self._hour_col = hour_col

        def __call__(self, row: operations.TRow) -> operations.TRowsGenerator:
            enter_dt = datetime.strptime(row[self._enter_col], '%Y%m%dT%H%M%S.%f')
            leave_dt = datetime.strptime(row[self._leave_col], '%Y%m%dT%H%M%S.%f')
            duration = (leave_dt - enter_dt).total_seconds()
            weekday = calendar.day_abbr[enter_dt.weekday()]
            new_row = dict(row)
            new_row['duration'] = duration
            new_row[self._weekday_col] = weekday
            new_row[self._hour_col] = enter_dt.hour
            yield new_row

    class SpeedMapper(operations.Mapper):
        def __init__(self, distance_col: str, duration_col: str, result_col: str) -> None:
            self._distance_col = distance_col
            self._duration_col = duration_col
            self._result_col = result_col

        def __call__(self, row: operations.TRow) -> operations.TRowsGenerator:
            duration_hours = row[self._duration_col] / 3600
            speed = row[self._distance_col] / duration_hours if duration_hours else 0
            new_row = dict(row)
            new_row[self._result_col] = speed
            yield new_row

    class AvgReducer(operations.Reducer):
        def __init__(self, column: str) -> None:
            self._column = column

        def __call__(self, group_key: tuple[str, ...], rows: operations.TRowsIterable) -> operations.TRowsGenerator:
            total = 0.0
            cnt = 0
            sample_row: operations.TRow | None = None
            for row in rows:
                cnt += 1
                total += row[self._column]
                sample_row = row

            if sample_row is None or cnt == 0:
                return

            new_row: operations.TRow = {k: sample_row[k] for k in sample_row if k in group_key}
            new_row[self._column] = total / cnt
            yield new_row

    time_graph = Graph.graph_from_iter(input_stream_name_time) \
        .map(ExtractTime(enter_time_column, leave_time_column, weekday_result_column, hour_result_column)) \
        .sort([edge_id_column])

    length_graph = Graph.graph_from_iter(input_stream_name_length) \
        .map(EnrichLength(start_coord_column, end_coord_column)) \
        .sort([edge_id_column])

    return time_graph \
        .join(operations.InnerJoiner(), length_graph, [edge_id_column]) \
        .map(SpeedMapper('distance_km', 'duration', speed_result_column)) \
        .sort([weekday_result_column, hour_result_column]) \
        .reduce(AvgReducer(speed_result_column), [weekday_result_column, hour_result_column])
