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
import pytest

from pyfxlib.schema import LiteralType, QueryAnswer

# to_pandas only reads `meta` and `rows`, but `query` is required, so every fixture needs one.
_PIPELINE = {"stages": [{"kind": "source", "table": {"kind": "products"}}]}


def _answer(
    columns: Sequence[tuple[str, LiteralType]], rows: Sequence[Sequence[Any]]
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
    (LiteralType.INTEGER, "Int64", 42),
    (LiteralType.REAL, "float64", 1.5),
    (LiteralType.STRING, "string", "a"),
    (LiteralType.BOOLEAN, "boolean", True),
    (LiteralType.DATE_ONLY, "datetime64[ns]", "2026-09-22"),
    (LiteralType.DATE_TIME, "datetime64[ns]", "2026-09-22T10:30:00"),
    (LiteralType.OTHER, "object", {"key": 1}),
]


@pytest.mark.parametrize("column_type,dtype,value", _COLUMN_TYPES)
def test_to_pandas_maps_each_column_type_to_its_dtype(
    column_type: LiteralType, dtype: str, value: Any
) -> None:
    series = _answer([("c", column_type)], [[value]]).to_pandas()["c"]

    assert str(series.dtype) == dtype


# TODO: This is a current gap in query.py, will be fixed in next MR
@pytest.mark.parametrize("column_type,dtype,value", _COLUMN_TYPES)
def test_to_pandas_keeps_nulls_missing(column_type: LiteralType, dtype: str, value: Any) -> None:
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
    series = _answer([("c", LiteralType.STRING)], [["a"], [None]]).to_pandas()["c"]

    assert series.dropna().tolist() == ["a"]


def test_to_pandas_keeps_a_null_boolean_missing_rather_than_false() -> None:
    """A bool cast makes a NULL indistinguishable from a genuine False."""
    series = _answer([("c", LiteralType.BOOLEAN)], [[True], [None], [False]]).to_pandas()["c"]

    assert series.isna().tolist() == [False, True, False]
    assert series.dropna().tolist() == [True, False]


def test_to_pandas_leaves_structured_values_alone() -> None:
    """OTHER must not be stringified: a dict would become its repr."""
    series = _answer([("c", LiteralType.OTHER)], [[{"key": 1}], [[1, 2]], [None]]).to_pandas()["c"]

    assert series.iloc[0] == {"key": 1}
    assert series.iloc[1] == [1, 2]
    assert series.iloc[2] is None


def test_to_pandas_takes_column_names_and_order_from_the_metadata() -> None:
    answer = _answer([("z", LiteralType.INTEGER), ("a", LiteralType.STRING)], [[1, "x"], [2, "y"]])

    frame = answer.to_pandas()

    assert list(frame.columns) == ["z", "a"]
    assert frame["z"].tolist() == [1, 2]
    assert frame["a"].tolist() == ["x", "y"]


def test_to_pandas_handles_an_empty_result_set() -> None:
    """No rows still yields the declared columns and dtypes, so callers can concatenate."""
    frame = _answer([("a", LiteralType.INTEGER), ("b", LiteralType.STRING)], []).to_pandas()

    assert frame.shape == (0, 2)
    assert list(frame.columns) == ["a", "b"]
    assert [str(dtype) for dtype in frame.dtypes] == ["Int64", "string"]


# TODO: This is a current gap in query.py, will be fixed in next MR
def test_to_pandas_handles_a_mix_of_column_types_in_one_answer() -> None:
    answer = _answer(
        [
            ("qty", LiteralType.INTEGER),
            ("price", LiteralType.REAL),
            ("sku", LiteralType.STRING),
            ("active", LiteralType.BOOLEAN),
            ("valid_from", LiteralType.DATE_ONLY),
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
