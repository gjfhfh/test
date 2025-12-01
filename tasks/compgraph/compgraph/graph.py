from __future__ import annotations

import typing as tp

from . import operations as ops
from .external_sort import ExternalSort

Builder = tp.Callable[..., ops.TRowsIterable]


class Graph:
    """Computation graph built from a chain of operations."""

    def __init__(self, builder: Builder) -> None:
        self._builder = builder

    @staticmethod
    def graph_from_iter(name: str) -> 'Graph':
        """Create graph that reads rows from iterator factory provided to :meth:`run`.

        Parameters
        ----------
        name:
            Keyword argument name with callable returning iterator over rows.
        """

        def builder(**kwargs: tp.Any) -> ops.TRowsIterable:
            return ops.ReadIterFactory(name)(**kwargs)

        return Graph(builder)

    @staticmethod
    def graph_from_file(filename: str, parser: tp.Callable[[str], ops.TRow]) -> 'Graph':
        """Create graph reading rows from file using provided parser."""

        def builder(**_kwargs: tp.Any) -> ops.TRowsIterable:
            return ops.Read(filename, parser)(**_kwargs)

        return Graph(builder)

    def map(self, mapper: ops.Mapper) -> 'Graph':
        """Extend graph with :class:`operations.Map` step."""

        def builder(**kwargs: tp.Any) -> ops.TRowsIterable:
            return ops.Map(mapper)(self._builder(**kwargs))

        return Graph(builder)

    def reduce(self, reducer: ops.Reducer, keys: tp.Sequence[str]) -> 'Graph':
        """Extend graph with :class:`operations.Reduce` step."""

        def builder(**kwargs: tp.Any) -> ops.TRowsIterable:
            return ops.Reduce(reducer, keys)(self._builder(**kwargs))

        return Graph(builder)

    def sort(self, keys: tp.Sequence[str]) -> 'Graph':
        """Extend graph with external sort step."""

        sort_op = ExternalSort(tuple(keys))

        def builder(**kwargs: tp.Any) -> ops.TRowsIterable:
            return sort_op(self._builder(**kwargs))

        return Graph(builder)

    def join(self, joiner: ops.Joiner, join_graph: 'Graph', keys: tp.Sequence[str]) -> 'Graph':
        """Extend graph with join against another graph."""

        def builder(**kwargs: tp.Any) -> ops.TRowsIterable:
            return ops.Join(joiner, keys)(self._builder(**kwargs), join_graph._builder(**kwargs))

        return Graph(builder)

    def run(self, **kwargs: tp.Any) -> ops.TRowsIterable:
        """Start graph execution with provided data sources."""

        return self._builder(**kwargs)
