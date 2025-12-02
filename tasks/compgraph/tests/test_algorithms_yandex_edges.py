
from compgraph import algorithms


def materialize(graph, **kwargs):
    return list(graph.run(**kwargs))


def test_yandex_maps_filters_zero_length_and_duration():
    graph = algorithms.yandex_maps_graph(
        "time_stream",
        "length_stream",
        enter_time_column="enter_time",
        leave_time_column="leave_time",
        edge_id_column="edge_id",
        start_coord_column="start",
        end_coord_column="end",
    )

    times = [
        {"edge_id": 1, "enter_time": "", "leave_time": ""},
        {"edge_id": 1, "enter_time": "20170101T000000.000000", "leave_time": "20161231T235959.000000"},
    ]
    lengths = [
        {"edge_id": 1, "start": (0.0, 0.0), "end": (0.0, 0.0)},
        {"edge_id": 1, "length": "abc", "start": (0.0, 0.0), "end": (0.0, 0.1)},
    ]

    result = materialize(
        graph,
        time_stream=lambda: iter(times),
        length_stream=lambda: iter(lengths),
    )
    assert result == []


def test_yandex_maps_computes_speed_from_length_value():
    graph = algorithms.yandex_maps_graph(
        "time_stream",
        "length_stream",
        enter_time_column="enter_time",
        leave_time_column="leave_time",
        edge_id_column="edge_id",
        start_coord_column="start",
        end_coord_column="end",
    )

    times = [
        {
            "edge_id": 2,
            "enter_time": "20171020T112237.000000",
            "leave_time": "20171020T122237.000000",
        }
    ]
    lengths = [
        {"edge_id": 2, "length": 1000.0, "start": (0.0, 0.0), "end": (0.0, 0.0)}
    ]

    result = materialize(
        graph,
        time_stream=lambda: iter(times),
        length_stream=lambda: iter(lengths),
    )
    assert result == [
        {"weekday": "Fri", "hour": 11, "speed": 1.0}
    ]
