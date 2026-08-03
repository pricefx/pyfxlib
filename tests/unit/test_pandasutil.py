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

# pylint: disable=redefined-outer-name
import datetime
from typing import Any

import fastavro
import pandas as pd
import pytest

from pyfxlib.lowlevel.avro import AvroStream
from pyfxlib.lowlevel.pandasutil import (
    check_backend_supports_field_types,
    FieldSpecs,
    to_field_collection_spec,
)

UNSUPPORTED_DATA = [
    [[1, 2]],
    [12 + 1j * 3],
    [(1, 3)],
    [{"a": {"c": "foo", "d": 42}}],
]


def expected_type(col: pd.Series):
    return col.iloc[0].dtype if hasattr(col.iloc[0], "dtype") else type(col.iloc[0])


@pytest.mark.parametrize(
    "unsupported_data",
    [(data,) for data in UNSUPPORTED_DATA],
)
def test_pandas_to_avro_should_fail_if_unsupported_data_type_is_given(unsupported_data):
    col_name = "column_name"
    data = pd.DataFrame(data={col_name: unsupported_data})
    with pytest.raises(ValueError) as err:
        avro_stream = AvroStream.from_dataframe(data)
        [record for record in fastavro.reader(avro_stream)]

    assert (
        err.value.args[0]
        == f"unsupported type for column '{col_name}': '{expected_type(data[col_name])}'"
    )


@pytest.mark.parametrize(
    "valid_data, pd_to_avro_value, pd_dtype",
    [
        ([1, 2], lambda x: x, None),
        (
            [pd.to_datetime("2020-01-02T02:02:01.123", utc=True)],
            lambda x: x.to_pydatetime(),
            None,
        ),
        ([datetime.date(2020, 1, 2)], lambda x: x, None),
        (["a", None], lambda x: x, None),
        (["a", None], lambda x: x, pd.StringDtype()),
        ([3.14, 12.42, None], lambda x: x, None),
        ([True, False, None], lambda x: x, None),
    ],
)
def test_pandas_to_avro_should_work_with_valid_data_type(valid_data, pd_to_avro_value, pd_dtype):
    def na_as_none(val: Any) -> Any:
        return val if pd.notna(val) else None

    col_name = "column_name"
    data = pd.DataFrame(data={col_name: valid_data}, dtype=pd_dtype)
    avro_stream = AvroStream.from_dataframe(data)
    avro_reader = fastavro.reader(avro_stream)
    values = [na_as_none(record[col_name]) for record in avro_reader]
    expected_values = [na_as_none(pd_to_avro_value(data)) for data in valid_data]
    assert values == expected_values


def test_pandas_to_avro_should_work_thousands_of_lines():
    col_name = "column_name"
    original_data = [idx for idx in range(0, 10000)]
    data = pd.DataFrame(data={col_name: original_data})
    avro_stream = AvroStream.from_dataframe(data)
    avro_reader = fastavro.reader(avro_stream)
    values = [record[col_name] for record in avro_reader]
    assert values == original_data


@pytest.mark.parametrize(
    "unsupported_data",
    [(data,) for data in UNSUPPORTED_DATA],
)
def test_to_field_collection_spec_should_fail_if_unsupported_data_type_is_given(
    unsupported_data,
):
    col_name = "column_name"
    dataframe = pd.DataFrame(data={col_name: unsupported_data})
    with pytest.raises(ValueError) as err:
        to_field_collection_spec(dataframe)

    assert (
        err.value.args[0]
        == f"unsupported types for columns: ('{col_name}': '{expected_type(dataframe[col_name])}')"
    )


def test_to_field_collection_spec_should_list_all_the_columns_containing_unsupported_data_type():
    data = {f"col_{idx}": unsupported_data for idx, unsupported_data in enumerate(UNSUPPORTED_DATA)}
    dataframe = pd.DataFrame(data=data)
    with pytest.raises(ValueError) as err:
        to_field_collection_spec(dataframe)

    type_errors = ", ".join(
        [f"('{col_name}': '{expected_type(dataframe[col_name])}')" for col_name in data.keys()]
    )
    assert err.value.args[0] == f"unsupported types for columns: {type_errors}"


@pytest.mark.parametrize(
    "unsupported_data, inplace",
    [(data, inplace) for data in UNSUPPORTED_DATA for inplace in [True, False]],
)
def test_to_field_collection_spec_should_drop_invalid_data_when_on_unsupported_data_set_to_drop(
    unsupported_data, inplace
):
    col_name = "column_name"
    initial_df = pd.DataFrame(
        data={
            col_name: unsupported_data,
            "valid_col": [idx for idx in range(0, len(unsupported_data))],
        }
    )

    spec, res_df = to_field_collection_spec(initial_df, on_unsupported_type="drop", inplace=inplace)

    initial_df_expected_cols = (
        # columns are not dropped, just removed from result view whereas reset_index is done inplace
        ["index", "column_name", "valid_col"]
        if inplace
        else ["column_name", "valid_col"]
    )
    assert list(initial_df.columns) == initial_df_expected_cols
    assert initial_df.shape == (len(unsupported_data), len(initial_df_expected_cols))

    assert list(res_df.columns) == ["index", "valid_col"]
    assert res_df.shape == (len(unsupported_data), 2)

    assert len(spec) == 2
    assert spec[0]["name"] == "index"
    assert spec[1]["name"] == "valid_col"


@pytest.mark.parametrize(
    "unsupported_data, inplace",
    [(data, inplace) for data in UNSUPPORTED_DATA for inplace in [True, False]],
)
def test_to_field_collection_spec_should_coerce_invalid_data_when_on_unsupported_data_is_coerce(
    unsupported_data, inplace
):
    col_name = "column_name"
    dataframe = pd.DataFrame(data={col_name: unsupported_data})

    spec, res_df = to_field_collection_spec(
        dataframe, on_unsupported_type="coerce", inplace=inplace
    )

    def assert_correctly_coerced(data: pd.DataFrame):
        assert list(data.columns) == ["index", "column_name"]
        assert data.shape == (len(unsupported_data), 2)
        assert data[col_name].apply(lambda x: isinstance(x, str)).all()

    if inplace:
        assert_correctly_coerced(res_df)
        assert_correctly_coerced(dataframe)
    else:
        assert_correctly_coerced(res_df)
        assert list(dataframe.columns) == ["column_name"]
        assert dataframe.shape == (len(unsupported_data), 1)
        assert dataframe[col_name].apply(lambda x: isinstance(x, type(unsupported_data[0]))).all()

    assert len(spec) == 2
    assert spec[0]["name"] == "index"
    assert spec[1]["name"] == col_name
    assert spec[1]["type"] == "TEXT"


@pytest.mark.parametrize(
    "index_name, col_name, column_labels, expected_columns_labels",
    [
        (
            "the_index_name",
            "column_name",
            {
                "column_name": "a column label",
                "the_index_name": "the label for the index",
            },
            {
                "column_name": "a column label",
                "the_index_name": "the label for the index",
            },
        ),
        (
            "the_index_name",
            "column_name",
            {
                "column_name": "a column label",
            },
            {
                "column_name": "a column label",
                "the_index_name": "the_index_name",
            },
        ),
        (
            "the_index_name",
            "column_name",
            {
                "the_index_name": "the label for the index",
            },
            {
                "column_name": "column_name",
                "the_index_name": "the label for the index",
            },
        ),
        (
            "the_index_name",
            "column_name",
            None,
            {
                "column_name": "column_name",
                "the_index_name": "the_index_name",
            },
        ),
        (
            "the_index_name",
            "column_name",
            {},
            {
                "column_name": "column_name",
                "the_index_name": "the_index_name",
            },
        ),
    ],
)
def test_to_field_collection_spec_should_generate_field_labels(
    index_name, col_name, column_labels, expected_columns_labels
):
    dataframe = pd.DataFrame(data={col_name: ["foo", "bar"]})
    dataframe.index.name = index_name

    spec, res_df = to_field_collection_spec(
        dataframe, on_unsupported_type="coerce", column_labels=column_labels
    )

    assert len(spec) == 2
    assert spec[0]["name"] == index_name
    assert spec[0]["label"] == expected_columns_labels[index_name]
    assert spec[1]["name"] == col_name
    assert spec[1]["label"] == expected_columns_labels[col_name]


def test_to_field_collection_spec_should_raise_when_columns_label_contains_unknown_column():
    unknown_col_name = "an_unknown_col"
    column_labels = {
        unknown_col_name: "a unknown label",
        "index": "the label for the index",
    }
    dataframe = pd.DataFrame(data={"col1_name": ["foo", "bar"]})

    with pytest.raises(ValueError) as error:
        to_field_collection_spec(dataframe, column_labels=column_labels)

    assert (
        error.value.args[0]
        == f"column_labels contains at least one unknown column name: {[unknown_col_name]}"
    )


def test_field_specs_should_accept_lob_as_a_manual_field_type_treated_as_string():
    # LOB is a valid, manually-settable field type (it must not raise "unsupported FieldType")
    specs = FieldSpecs()
    specs.set_col_specs("bigtext", type="LOB")
    assert specs.schema["bigtext"]["type"] == "LOB"

    # and it is handled exactly like TEXT: same dtype conversion + check
    field_type_to_dtype = FieldSpecs().field_type_to_dtype
    assert field_type_to_dtype["LOB"] == field_type_to_dtype["TEXT"]

    # so a long (>255 char) string column satisfies the LOB check unchanged
    long_text_column = pd.Series(["x" * 1000], dtype="string")
    assert field_type_to_dtype["LOB"].check(long_text_column)


def test_field_specs_should_reject_unknown_field_type():
    # sanity check that the validation is still active for genuinely unknown types
    with pytest.raises(ValueError, match="unsupported FieldType"):
        FieldSpecs().set_col_specs("col", type="NOT_A_TYPE")


def _version(spec: str) -> dict[str, int | None]:
    parts = [int(p) for p in spec.split(".")]
    return {"major": None, "minor": None, "patch": None} | dict(
        zip(["major", "minor", "patch"], parts)
    )


@pytest.mark.parametrize(
    "backend, supported",
    [
        # major 16: fixed from 16.3.13; monotonic within the major
        ("16.3.12", False),
        ("16.3.13", True),
        ("16.3.14", True),
        ("16.2.99", False),  # below the 16.x floor
        ("16.4.0", True),  # any 16.x above the floor is fixed
        # major 17: 17.0.3 numerically higher than 16.3.13 but still lacks the fix
        ("17.0.3", False),
        ("17.0.4", True),
        ("17.5.0", True),  # any 17.x above the 17.x floor
        # majors newer than every declared bound are assumed to carry the fix
        ("18.0.0", True),
        # older majors are unsupported
        ("15.2.0", False),
    ],
)
def test_lob_push_is_gated_per_major_version(backend, supported):
    # a LOB field is only pushable to a backend that carries the per-major fix; the check is
    # NOT a scalar floor (17.0.3 > 16.3.13 yet is unsupported)
    fields_spec = [{"name": "bigtext", "type": "LOB"}]
    if supported:
        check_backend_supports_field_types(fields_spec, lambda: _version(backend))
    else:
        with pytest.raises(RuntimeError, match="requires a newer Pricefx core"):
            check_backend_supports_field_types(fields_spec, lambda: _version(backend))


def test_non_gated_field_types_never_query_the_backend_version():
    # a push without any version-gated field must not pay a backend_version round-trip
    def _must_not_be_called() -> dict[str, int | None]:
        raise AssertionError("backend_version must not be queried for non-gated types")

    fields_spec = [{"name": "a", "type": "TEXT"}, {"name": "b", "type": "NUMBER"}]
    check_backend_supports_field_types(fields_spec, _must_not_be_called)
