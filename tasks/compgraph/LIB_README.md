# Compgraph

Учебная библиотека для построения и исполнения потоковых вычислительных графов над таблицами.  Операции `map`, `reduce`, `sort` и `join` комбинируются в описаниях графов (см. `compgraph/graph.py` и `compgraph/operations.py`), после чего граф можно многократно запускать на разных источниках данных.

## Установка

```bash
uv pip install --system -e .
```

Для локальной проверки покрытия можно поставить dev-зависимости:

```bash
uv pip install --system '.[dev]'
```

В репозитории есть лёгкая заглушка `psutil` (см. `psutil.py`), поэтому тесты запускаются даже без внешнего пакета.

## Алгоритмы

В `compgraph/algorithms.py` реализованы готовые графы:

* `word_count_graph` — классический Word Count.
* `inverted_index_graph` — инвертированный индекс с TF-IDF.
* `pmi_graph` — топ слов по PMI для каждого документа.
* `yandex_maps_graph` — средняя скорость движения по часу и дню недели.

Каждая функция возвращает объект `Graph`, который можно запустить, передав фабрики итераторов для входных потоков.

## Примеры

В папке `examples` лежат готовые CLI-скрипты (используют стандартный `argparse`). Везде вход/выход — JSONL.

```bash
python examples/run_word_count.py --input path/to/texts.jsonl --output wc.jsonl
python examples/run_inverted_index.py --input path/to/texts.jsonl --output tfidf.jsonl
python examples/run_pmi.py --input path/to/texts.jsonl --output pmi.jsonl
python examples/run_yandex_maps.py --travel-times path/to/travel.jsonl --edges path/to/edges.jsonl --output speeds.jsonl
```

Вывод можно направить в stdout, указав `--output -`.

## Тестирование

```bash
pytest
```

Для проверки покрытия (понадобится `pytest-cov` из dev-зависимостей):

```bash
pytest --cov=compgraph --cov-report=term-missing
```

## Структура проекта

* `compgraph/graph.py` — описание интерфейса графа.
* `compgraph/operations.py` — мапперы, редьюсеры и джойнеры.
* `compgraph/algorithms.py` — реализованные задачи.
* `examples/` — CLI-скрипты для запуска алгоритмов.
* `tests/` — полный набор unit-тестов (авторские + дополнительные для CLI).
