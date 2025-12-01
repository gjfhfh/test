from __future__ import annotations

from abc import ABC, abstractmethod
import heapq
import string
import typing as tp

TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    """Base operation in computation graph."""

    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """Run operation over provided rows."""
        raise NotImplementedError


class Read(Operation):
    """Read rows from file using provided parser."""

    def __init__(self, filename: str, parser: tp.Callable[[str], TRow]) -> None:
        self._filename = filename
        self._parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:  # type: ignore[override]
        with open(self._filename) as f:
            for line in f:
                yield self._parser(line)


class ReadIterFactory(Operation):
    """Read rows from iterator factory passed in kwargs."""

    def __init__(self, name: str) -> None:
        self._name = name

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:  # type: ignore[override]
        for row in kwargs[self._name]():
            yield row


class Mapper(ABC):
    """Base class for mappers."""

    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """Process single row and yield zero or more rows."""
        raise NotImplementedError


class Map(Operation):
    """Apply mapper to each row from upstream iterator."""

    def __init__(self, mapper: Mapper) -> None:
        self._mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:  # type: ignore[override]
        for row in rows:
            for new_row in self._mapper(row):
                yield new_row


class Reducer(ABC):
    """Base class for reducers."""

    @abstractmethod
    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        """Process rows for a single group defined by ``group_key``."""
        raise NotImplementedError


class Reduce(Operation):
    """Group rows by keys and apply reducer for every group."""

    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        self._reducer = reducer
        self._keys = tuple(keys)

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:  # type: ignore[override]
        rows_iter = iter(rows)
        try:
            first = next(rows_iter)
        except StopIteration:
            return

        def make_key(row: TRow) -> tuple[tp.Any, ...]:
            return tuple(row[k] for k in self._keys)

        current_key = make_key(first)
        buffered = first
        finished = False

        while True:

            def group_gen() -> TRowsGenerator:
                nonlocal buffered, current_key, finished
                row = buffered
                key_val = current_key
                while True:
                    yield row
                    try:
                        row = next(rows_iter)
                    except StopIteration:
                        finished = True
                        buffered = None
                        current_key = ()
                        return
                    k = make_key(row)
                    if k != key_val:
                        buffered = row
                        current_key = k
                        return

            group_iterator = group_gen()
            for new_row in self._reducer(self._keys, group_iterator):
                yield new_row
            for _ in group_iterator:
                pass

            if finished or buffered is None:
                break


class Joiner(ABC):
    """Base class for joiners."""

    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    @abstractmethod
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        """Join two sorted streams ``rows_a`` and ``rows_b`` by ``keys``."""
        raise NotImplementedError


class Join(Operation):
    """Join two upstream iterables with provided joiner."""

    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self._keys = keys
        self._joiner = joiner

    def __call__(self, rows_a: TRowsIterable, rows_b: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:  # type: ignore[override]
        for row in self._joiner(self._keys, rows_a, rows_b):
            yield row


class DummyMapper(Mapper):
    """Yield exactly the row passed."""

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield dict(row)


class FirstReducer(Reducer):
    """Yield only first row from passed ones."""

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:  # type: ignore[override]
        for row in rows:
            yield dict(row)
            break


class FilterPunctuation(Mapper):
    """Leave only non-punctuation symbols in ``column`` value."""

    def __init__(self, column: str):
        self._column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        value = row[self._column]
        filter_value = value.translate(str.maketrans('', '', string.punctuation))
        new_row = dict(row)
        new_row[self._column] = filter_value
        yield new_row


class LowerCase(Mapper):
    """Replace column value with value in lower case."""

    def __init__(self, column: str):
        self._column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        value = row[self._column]
        lower_value = value.lower()
        new_row = dict(row)
        new_row[self._column] = lower_value
        yield new_row


class Split(Mapper):
    """Split row on multiple rows by separator."""

    def __init__(self, column: str, separator: str | None = None) -> None:
        self._column = column
        self._separator = separator

    def __call__(self, row: TRow) -> TRowsGenerator:
        val = row.get(self._column)
        if not isinstance(val, str):
            yield row
            return

        if self._separator is None:
            parts = val.split()
        else:
            if self._separator == "":
                parts = list(val) if val else [""]
            else:
                parts = val.split(self._separator)

        for p in parts:
            new = dict(row)
            new[self._column] = p
            yield new


class Product(Mapper):
    """Calculates product of multiple columns."""

    def __init__(self, columns: tp.Sequence[str], result_column: str = 'product') -> None:
        self._columns = columns
        self._result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        new_row = dict(row)
        result_value = 1
        for column in self._columns:
            result_value *= row[column]
        new_row[self._result_column] = result_value
        yield new_row


class Filter(Mapper):
    """Remove records that don't satisfy some condition."""

    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        self._condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self._condition(row):
            new_row = dict(row)
            yield new_row


class Project(Mapper):
    """Leave only mentioned columns."""

    def __init__(self, columns: tp.Sequence[str]) -> None:
        self._columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        new_row: TRow = {}
        for column in self._columns:
            new_row[column] = row[column]
        yield new_row


class TopN(Reducer):
    """Calculate top N by value in ``column``."""

    def __init__(self, column: str, n: int) -> None:
        self._column_max = column
        self._n = n

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:  # type: ignore[override]
        heap: list[tuple[tp.Any, int, TRow]] = []

        for i, row in enumerate(rows):
            value = row[self._column_max]
            item = dict(row)
            if len(heap) < self._n:
                heapq.heappush(heap, (value, i, item))
            else:
                if value > heap[0][0]:
                    heapq.heapreplace(heap, (value, i, item))

        for _, _, row in sorted(heap, key=lambda x: x[0], reverse=True):
            yield row


class TermFrequency(Reducer):
    """Calculate frequency of values in column for each group."""

    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        self._words_column = words_column
        self._result_column = result_column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:  # type: ignore[override]
        total = 0
        counts: dict[tp.Any, int] = {}
        first_row: TRow | None = None

        for row in rows:
            if first_row is None:
                first_row = row
            word = row[self._words_column]
            counts[word] = counts.get(word, 0) + 1
            total += 1

        if first_row is None or total == 0:
            return

        base: TRow = {}
        for k, v in first_row.items():
            if k not in (self._words_column, 'count'):
                base[k] = v

        for word, c in counts.items():
            new_row = dict(base)
            new_row[self._words_column] = word
            new_row[self._result_column] = c / total
            yield new_row


class Count(Reducer):
    """Count records by key."""

    def __init__(self, column: str) -> None:
        self._column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:  # type: ignore[override]
        cnt = 0
        saved_row: TRow | None = None
        for row in rows:
            cnt += 1
            saved_row = row

        if saved_row is None:
            return

        new_row: TRow = {key: saved_row[key] for key in saved_row if key in group_key}
        new_row[self._column] = cnt
        yield new_row


class Sum(Reducer):
    """Sum values aggregated by key."""

    def __init__(self, column: str) -> None:
        self._column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:  # type: ignore[override]
        total = 0
        saved_row: TRow | None = None
        for row in rows:
            total += row[self._column]
            saved_row = row

        if saved_row is None:
            return

        new_row: TRow = {key: saved_row[key] for key in saved_row if key in group_key}
        new_row[self._column] = total
        yield new_row


def _row_key(row: TRow, keys: tp.Sequence[str]) -> tuple:
    return tuple(row[k] for k in keys)


def _key_by(row: TRow, keys: tp.Sequence[str]) -> tuple[tp.Any, ...]:
    return tuple(row[k] for k in keys)


def _merge_rows(
    keys: tp.Sequence[str],
    row_a: TRow,
    row_b: TRow,
    suffix_a: str,
    suffix_b: str,
) -> TRow:
    res: TRow = {}
    key_set = set(keys)

    for k, v in row_a.items():
        if k in key_set:
            res[k] = v
        elif k in row_b:
            res[k + suffix_a] = v
        else:
            res[k] = v

    for k, v in row_b.items():
        if k in key_set:
            continue
        elif k in row_a:
            res[k + suffix_b] = v
        else:
            res[k] = v

    return res


class InnerJoiner(Joiner):
    """Join with inner strategy."""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:  # type: ignore[override]
        it_a = iter(rows_a)
        it_b = iter(rows_b)

        try:
            a = next(it_a)
        except StopIteration:
            return

        try:
            b = next(it_b)
        except StopIteration:
            return

        ka = _key_by(a, keys)
        kb = _key_by(b, keys)

        while True:
            if ka < kb:
                try:
                    a = next(it_a)
                    ka = _key_by(a, keys)
                except StopIteration:
                    return
            elif ka > kb:
                try:
                    b = next(it_b)
                    kb = _key_by(b, keys)
                except StopIteration:
                    return
            else:
                current_key = ka

                group_a = [a]
                while True:
                    try:
                        na = next(it_a)
                    except StopIteration:
                        na = None
                        break
                    if _key_by(na, keys) == current_key:
                        group_a.append(na)
                    else:
                        break

                while True:
                    for aa in group_a:
                        yield _merge_rows(keys, aa, b, self._a_suffix, self._b_suffix)

                    try:
                        nb = next(it_b)
                    except StopIteration:
                        nb = None
                        break

                    if _key_by(nb, keys) == current_key:
                        b = nb
                        continue
                    else:
                        b = nb
                        kb = _key_by(b, keys)
                        break

                if na is None or b is None:
                    return

                a = na
                ka = _key_by(a, keys)


class OuterJoiner(Joiner):
    """Join with outer strategy."""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:  # type: ignore[override]
        dict_a: dict[tuple[tp.Any, ...], list[TRow]] = {}
        dict_b: dict[tuple[tp.Any, ...], list[TRow]] = {}

        for a in rows_a:
            key_a = _row_key(a, keys)
            if key_a not in dict_a:
                dict_a[key_a] = []
            dict_a[key_a].append(a)

        for b in rows_b:
            key_b = _row_key(b, keys)
            if key_b not in dict_b:
                dict_b[key_b] = []
            dict_b[key_b].append(b)

        all_keys = set(dict_a.keys()) | set(dict_b.keys())

        for key in all_keys:
            list_a = dict_a.get(key, [])
            list_b = dict_b.get(key, [])

            if not list_a:
                for b in list_b:
                    yield dict(b)
                continue

            if not list_b:
                for a in list_a:
                    yield dict(a)
                continue

            for a in list_a:
                for b in list_b:
                    yield {**a, **b}


class LeftJoiner(Joiner):
    """Join with left strategy."""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:  # type: ignore[override]
        it_a = iter(rows_a)
        it_b = iter(rows_b)

        try:
            a = next(it_a)
        except StopIteration:
            return

        try:
            b = next(it_b)
        except StopIteration:
            yield dict(a)
            for a in it_a:
                yield dict(a)
            return

        ka = _key_by(a, keys)
        kb = _key_by(b, keys)

        while True:
            if ka < kb:
                current_key = ka
                yield dict(a)
                while True:
                    try:
                        na = next(it_a)
                    except StopIteration:
                        return
                    if _key_by(na, keys) == current_key:
                        yield dict(na)
                    else:
                        a = na
                        ka = _key_by(a, keys)
                        break

            elif ka > kb:
                current_key = kb
                while True:
                    try:
                        nb = next(it_b)
                    except StopIteration:
                        yield dict(a)
                        for a in it_a:
                            yield dict(a)
                        return
                    if _key_by(nb, keys) == current_key:
                        continue
                    else:
                        b = nb
                        kb = _key_by(b, keys)
                        break

            else:
                current_key = ka

                group_b = [b]
                while True:
                    try:
                        nb = next(it_b)
                    except StopIteration:
                        nb = None
                        break
                    if _key_by(nb, keys) == current_key:
                        group_b.append(nb)
                    else:
                        break

                while True:
                    for bb in group_b:
                        yield _merge_rows(keys, a, bb, self._a_suffix, self._b_suffix)

                    try:
                        na = next(it_a)
                    except StopIteration:
                        na = None
                        break

                    if _key_by(na, keys) == current_key:
                        a = na
                        continue
                    else:
                        break

                if na is None:
                    return

                if nb is None:
                    yield dict(na)
                    for a in it_a:
                        yield dict(a)
                    return

                a = na
                ka = _key_by(a, keys)
                b = nb
                kb = _key_by(b, keys)


class RightJoiner(Joiner):
    """Join with right strategy."""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:  # type: ignore[override]
        it_a = iter(rows_a)
        it_b = iter(rows_b)

        try:
            a = next(it_a)
        except StopIteration:
            for b in it_b:
                yield dict(b)
            return

        try:
            b = next(it_b)
        except StopIteration:
            return

        ka = _key_by(a, keys)
        kb = _key_by(b, keys)

        while True:
            if ka < kb:
                key_a = ka
                while True:
                    try:
                        na = next(it_a)
                    except StopIteration:
                        yield dict(b)
                        for b in it_b:
                            yield dict(b)
                        return
                    if _key_by(na, keys) == key_a:
                        continue
                    else:
                        a = na
                        ka = _key_by(a, keys)
                        break

            elif ka > kb:
                key_b = kb
                yield dict(b)
                while True:
                    try:
                        nb = next(it_b)
                    except StopIteration:
                        return
                    if _key_by(nb, keys) == key_b:
                        yield dict(nb)
                    else:
                        b = nb
                        kb = _key_by(b, keys)
                        break

            else:
                current_key = ka

                group_b = [b]
                while True:
                    try:
                        nb = next(it_b)
                    except StopIteration:
                        nb = None
                        break
                    if _key_by(nb, keys) == current_key:
                        group_b.append(nb)
                    else:
                        break

                while True:
                    for bb in group_b:
                        yield _merge_rows(keys, a, bb, self._a_suffix, self._b_suffix)

                    try:
                        na = next(it_a)
                    except StopIteration:
                        na = None
                        break

                    if _key_by(na, keys) == current_key:
                        a = na
                        continue
                    else:
                        a = na
                        ka = _key_by(a, keys)
                        break

                if nb is None:
                    return

                b = nb
                kb = _key_by(b, keys)
