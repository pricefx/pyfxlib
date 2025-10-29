# pylint: disable=redefined-outer-name
import datetime
from typing import Any

import fastavro

import pandas as pd

from pyfx2.lowlevel.avro import AvroStream
from pyfx2.lowlevel.pandasutil import to_field_collection_spec

import pytest


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
