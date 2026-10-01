# Copyright 2025-2026 Pricefx
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, Sequence

import pandas as pd
from pydantic import ValidationError
import pytest

from pyfxlib.schema import (
    Aggregate,
    AggregateScalarFunction,
    Filter,
    Frame,
    JoinTable,
    LiteralType,
    LiteralValue,
    PADataFeed,
    PADataSource,
    Pipeline,
    PreviousStageFunctionAgg,
    QueryAnswer,
    ResultColumnType,
    Source,
    Table,
)

# to_pandas only reads `meta` and `rows`, but `query` is required, so every fixture needs one.
_PIPELINE = {"stages": [{"kind": "source", "table": {"kind": "products"}}]}


def _answer(
    columns: Sequence[tuple[str, ResultColumnType]], rows: Sequence[Sequence[Any]]
) -> QueryAnswer:
    return QueryAnswer.model_validate(
        {
            "query": _PIPELINE,
            "meta": {"columns": [{"name": name, "type": type_} for name, type_ in columns]},
            "rows": rows,
        }
    )


# Every result column type, the pandas dtype it maps to, and a sample value of that type.
_COLUMN_TYPES = [
    (ResultColumnType.INTEGER, "Int64", 42),
    (ResultColumnType.REAL, "float64", 1.5),
    (ResultColumnType.STRING, "string", "a"),
    (ResultColumnType.BOOLEAN, "boolean", True),
    (ResultColumnType.DATE_ONLY, "datetime64[ns]", "2026-09-22"),
    (ResultColumnType.DATE_TIME, "datetime64[ns]", "2026-09-22T10:30:00"),
    (ResultColumnType.OTHER, "object", {"key": 1}),
]


@pytest.mark.parametrize("column_type,dtype,value", _COLUMN_TYPES)
def test_to_pandas_maps_each_column_type_to_its_dtype(
    column_type: ResultColumnType, dtype: str, value: Any
) -> None:
    series = _answer([("c", column_type)], [[value]]).to_pandas()["c"]

    assert str(series.dtype) == dtype


# TODO: This is a current gap in query.py, will be fixed in next MR
@pytest.mark.parametrize("column_type,dtype,value", _COLUMN_TYPES)
def test_to_pandas_keeps_nulls_missing(
    column_type: ResultColumnType, dtype: str, value: Any
) -> None:
    """A NULL must survive as NA.

    Builtin dtypes cannot represent one: `int` raises IntCastingNaNError, and `str` and
    `bool` coerce it silently. Any LEFT_OUTER join produces NULLs, so this is the common
    case rather than an edge one.
    """
    series = _answer([("c", column_type)], [[value], [None]]).to_pandas()["c"]

    assert str(series.dtype) == dtype
    assert series.isna().tolist() == [False, True]
    assert series.dropna().tolist() == [pd.Timestamp(value) if "datetime" in dtype else value]


# TODO: This is a current gap in query.py, will be fixed in next MR
def test_to_pandas_keeps_a_null_string_missing_rather_than_the_text_none() -> None:
    """A str cast turns None into the four-character string 'None', which reads as data."""
    series = _answer([("c", ResultColumnType.STRING)], [["a"], [None]]).to_pandas()["c"]

    assert series.dropna().tolist() == ["a"]


def test_to_pandas_keeps_a_null_boolean_missing_rather_than_false() -> None:
    """A bool cast makes a NULL indistinguishable from a genuine False."""
    series = _answer([("c", ResultColumnType.BOOLEAN)], [[True], [None], [False]]).to_pandas()["c"]

    assert series.isna().tolist() == [False, True, False]
    assert series.dropna().tolist() == [True, False]


def test_to_pandas_leaves_structured_values_alone() -> None:
    """OTHER must not be stringified: a dict would become its repr."""
    series = _answer([("c", ResultColumnType.OTHER)], [[{"key": 1}], [[1, 2]], [None]]).to_pandas()[
        "c"
    ]

    assert series.iloc[0] == {"key": 1}
    assert series.iloc[1] == [1, 2]
    assert series.iloc[2] is None


def test_to_pandas_takes_column_names_and_order_from_the_metadata() -> None:
    answer = _answer(
        [("z", ResultColumnType.INTEGER), ("a", ResultColumnType.STRING)], [[1, "x"], [2, "y"]]
    )

    frame = answer.to_pandas()

    assert list(frame.columns) == ["z", "a"]
    assert frame["z"].tolist() == [1, 2]
    assert frame["a"].tolist() == ["x", "y"]


def test_to_pandas_handles_an_empty_result_set() -> None:
    """No rows still yields the declared columns and dtypes, so callers can concatenate."""
    frame = _answer(
        [("a", ResultColumnType.INTEGER), ("b", ResultColumnType.STRING)], []
    ).to_pandas()

    assert frame.shape == (0, 2)
    assert list(frame.columns) == ["a", "b"]
    assert [str(dtype) for dtype in frame.dtypes] == ["Int64", "string"]


# TODO: This is a current gap in query.py, will be fixed in next MR
def test_to_pandas_handles_a_mix_of_column_types_in_one_answer() -> None:
    answer = _answer(
        [
            ("qty", ResultColumnType.INTEGER),
            ("price", ResultColumnType.REAL),
            ("sku", ResultColumnType.STRING),
            ("active", ResultColumnType.BOOLEAN),
            ("valid_from", ResultColumnType.DATE_ONLY),
        ],
        [
            [2, 9.99, "SKU-1", True, "2026-09-22"],
            [None, None, None, None, None],
        ],
    )

    frame = answer.to_pandas()

    assert [str(dtype) for dtype in frame.dtypes] == [
        "Int64",
        "float64",
        "string",
        "boolean",
        "datetime64[ns]",
    ]
    assert frame.iloc[0].tolist() == [2, 9.99, "SKU-1", True, pd.Timestamp("2026-09-22")]
    assert frame.iloc[1].isna().all()


@pytest.mark.parametrize(
    "table,kind",
    [
        # Jackson type ids are case-sensitive, so the casing must match the DTO exactly.
        (PADataFeed(data_feed_unique_name="DF"), "dataFeed"),
        # `datasource` is the deprecated id (PFUN-35498).
        (PADataSource(data_source_unique_name="DS"), "dataSource"),
    ],
)
def test_table_dumps_the_kind_the_dto_expects(table: Table, kind: str) -> None:
    pipeline = Pipeline.model_validate({"stages": [{"kind": "source", "table": table}]})

    dumped = pipeline.model_dump(by_alias=True)

    assert dumped["stages"][0]["table"]["kind"] == kind
    assert Pipeline.model_validate(dumped) == pipeline


def test_aggregate_defaults_dimensions_and_function_arguments_to_empty() -> None:
    """The DTO defaults both to an empty list, so a bare `countAll()` total needs neither."""
    aggregate = Aggregate.model_validate(
        {
            "columns": [
                {"expression": {"kind": "function", "name": "countAll"}, "alias": "n"},
            ],
        }
    )

    assert aggregate.dimensions == []
    assert isinstance(aggregate.columns[0].expression, PreviousStageFunctionAgg)
    assert aggregate.columns[0].expression.arguments == []


def test_filter_accepts_a_function_without_arguments() -> None:
    criteria = {
        "kind": "function",
        "name": "lessThan",
        "arguments": [
            {"kind": "columnReference", "source": "previousStage", "column": "x"},
            {"kind": "function", "name": "random"},
        ],
    }

    assert Filter.model_validate({"criteria": criteria}).model_dump(by_alias=True)["criteria"][
        "arguments"
    ][1] == {"kind": "function", "name": "random", "arguments": []}


def test_join_defaults_columns_to_empty() -> None:
    join = JoinTable.model_validate(
        {
            "table": {"kind": "customers"},
            "type": "INNER",
            "criteria": {"kind": "literal", "type": "BOOLEAN", "value": True},
        }
    )

    assert join.columns == []


def test_datetime_literal_dumps_the_literal_spelling_not_the_result_column_one() -> None:
    """Literals say DATETIME while result columns say DATE_TIME: the two must not be mixed up."""
    literal = LiteralValue(type=LiteralType.DATETIME, value="2026-09-22T10:30:00")

    assert literal.model_dump(by_alias=True)["type"] == "DATETIME"
    with pytest.raises(ValidationError):
        LiteralValue.model_validate({"type": "DATE_TIME", "value": "2026-09-22T10:30:00"})


def _column(name: str) -> dict[str, Any]:
    return {"kind": "columnReference", "source": "previousStage", "column": name}


def _function(name: str, *arguments: dict[str, Any]) -> dict[str, Any]:
    return {"kind": "function", "name": name, "arguments": list(arguments)}


@pytest.mark.parametrize(
    "expression",
    [
        _function("div", _function("sum", _column("a")), _function("sum", _column("b"))),
        _function(
            "coalesce",
            _function("max", _column("a")),
            {"kind": "literal", "type": "INTEGER", "value": 0},
        ),
        _function("plus", _function("multiply", _function("avg", _column("a")), _column("b"))),
    ],
)
def test_aggregate_accepts_scalar_functions_over_aggregates(expression: dict[str, Any]) -> None:
    aggregate = Aggregate.model_validate({"columns": [{"expression": expression, "alias": "x"}]})

    assert isinstance(aggregate.columns[0].expression, AggregateScalarFunction)
    assert aggregate.model_dump(by_alias=True)["columns"][0]["expression"] == expression


def test_aggregate_rejects_an_aggregate_nested_in_an_aggregate() -> None:
    expression = _function("sum", _function("sum", _column("a")))

    with pytest.raises(ValidationError):
        Aggregate.model_validate({"columns": [{"expression": expression, "alias": "x"}]})


_UNBOUNDED_PRECEDING = {"type": "unbounded", "direction": "PRECEDING"}
_UNBOUNDED_FOLLOWING = {"type": "unbounded", "direction": "FOLLOWING"}
_CURRENT_ROW = {"type": "currentRow"}


def _frame(start: dict[str, Any], end: dict[str, Any]) -> dict[str, Any]:
    return {"type": "rows", "start": start, "end": end, "exclusion": "NONE"}


@pytest.mark.parametrize(
    "start,end",
    [
        (_UNBOUNDED_PRECEDING, _UNBOUNDED_FOLLOWING),
        ({"type": "offset", "offset": 2, "direction": "PRECEDING"}, _CURRENT_ROW),
        (_CURRENT_ROW, {"type": "offset", "offset": 1, "direction": "FOLLOWING"}),
    ],
)
def test_frame_round_trips_valid_bounds(start: dict[str, Any], end: dict[str, Any]) -> None:
    frame = Frame.model_validate(_frame(start, end))

    assert frame.model_dump(by_alias=True, mode="json") == _frame(start, end)


@pytest.mark.parametrize(
    "start,end",
    [
        (_UNBOUNDED_FOLLOWING, _CURRENT_ROW),
        (_CURRENT_ROW, _UNBOUNDED_PRECEDING),
    ],
)
def test_frame_rejects_bounds_the_dto_rejects(start: dict[str, Any], end: dict[str, Any]) -> None:
    """The DTO throws on a frame starting at the partition end or ending at its start."""
    with pytest.raises(ValidationError):
        Frame.model_validate(_frame(start, end))


def _string(value: str) -> dict[str, Any]:
    return {"kind": "literal", "type": "STRING", "value": value}


@pytest.mark.parametrize("name", ["in", "notIn"])
def test_in_accepts_an_expression_followed_by_literals(name: str) -> None:
    criteria = _function(name, _column("a"), _string("x"), _string("y"))

    assert (
        Filter.model_validate({"criteria": criteria}).model_dump(by_alias=True)["criteria"]
        == criteria
    )


@pytest.mark.parametrize(
    "arguments",
    [
        [_column("a"), _column("b")],
        [_column("a"), _string("x"), _function("upper", _string("y"))],
        [_column("a")],
    ],
    ids=["column value", "function value", "no value"],
)
@pytest.mark.parametrize("name", ["in", "notIn"])
def test_in_rejects_values_other_than_literals(name: str, arguments: list[dict[str, Any]]) -> None:
    """The DTO casts each value to a literal, so anything else fails server-side."""
    with pytest.raises(ValidationError):
        Filter.model_validate({"criteria": _function(name, *arguments)})


def test_in_values_are_checked_in_source_criteria_too() -> None:
    criteria = _function(
        "in",
        {"kind": "columnReference", "source": "table", "column": "a"},
        {"kind": "columnReference", "source": "table", "column": "b"},
    )

    with pytest.raises(ValidationError):
        Source.model_validate({"table": {"kind": "products"}, "criteria": criteria})
