import math

from compgraph import algorithms


def materialize(gen):
    return list(gen)


def test_yandex_maps_graph_computes_average_speed_for_hour_bucket():
    graph = algorithms.yandex_maps_graph(
        input_stream_name_time="time_rows",
        input_stream_name_length="length_rows",
        enter_time_column="enter",
        leave_time_column="leave",
        edge_id_column="edge",
        start_coord_column="start",
        end_coord_column="end",
        weekday_result_column="weekday",
        hour_result_column="hour",
        speed_result_column="speed",
    )

    time_rows = [
        {
            "edge": 1,
            "enter": "20240101T010000.000",
            "leave": "20240101T020000.000",
        }
    ]
    length_rows = [
        {"edge": 1, "length": 120},
    ]

    result = materialize(
        graph.run(
            time_rows=lambda: iter(time_rows),
            length_rows=lambda: iter(length_rows),
        )
    )

    assert result == [
        {
            "weekday": "Mon",
            "hour": 1,
            "speed": 0.12,
        }
    ]


def test_yandex_maps_graph_filters_invalid_intervals_and_lengths():
    graph = algorithms.yandex_maps_graph(
        input_stream_name_time="time_data",
        input_stream_name_length="length_data",
        enter_time_column="enter",
        leave_time_column="leave",
        edge_id_column="edge_id",
        start_coord_column="start",
        end_coord_column="end",
        weekday_result_column="weekday",
        hour_result_column="hour",
        speed_result_column="speed",
    )

    # one valid row, one with non-increasing timestamps (filtered), one with missing coords (length 0)
    time_data = [
        {"edge_id": 2, "enter": "20240102T030000.000", "leave": "20240102T040000.000"},
        {"edge_id": 2, "enter": "20240102T050000.000", "leave": "20240102T050000.000"},
    ]

    # using coordinates triggers haversine path; missing coords should be ignored by length filter
    length_data = [
        {"edge_id": 2, "start": (0.0, 0.0), "end": (0.0, math.radians(60))},
        {"edge_id": 3, "start": None, "end": None},
    ]

    result = materialize(
        graph.run(
            time_data=lambda: iter(time_data),
            length_data=lambda: iter(length_data),
        )
    )

    assert len(result) == 1
    assert result[0]["weekday"] == "Tue"
    assert result[0]["hour"] == 3
    assert math.isclose(result[0]["speed"], 116.44305488766722)
