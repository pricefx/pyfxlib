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

from collections.abc import Iterable
from io import StringIO
import json
from typing import Any

import numpy as np
import pandas as pd
import pytest

from pyfxlib._testtooling.conftest import (
    _async_conn,
    _auth,
    _conn,
    _connection_with_raising_session,
    _job_jst,
    _model_object,
    _partition,
    _pfx_base_url,
    _raising_auth,
    _raising_remote,
    _RaisingExceptionSession,
    _remote,
    _retry_and_raising_session,
    _session,
    _setup_datamart,
)
from pyfxlib._testtooling.helpers import (
    _calculation_results_as_dict,
    _csv_stream_to_dataframe,
)
from pyfxlib.api.domain import ModelObject, Partition, PlatformJob
from pyfxlib.lowlevel.avro import AvroStream
from pyfxlib.lowlevel.connection import ConnectionAsync
from pyfxlib.lowlevel.pandasutil import FieldSpecs
from pyfxlib.schema.query import AdvancedCriteria, FieldRule, FilterOperator, Operator

__all__ = [
    "_async_conn",
    "_auth",
    "_conn",
    "_connection_with_raising_session",
    "_partition",
    "_job_jst",
    "_model_object",
    "_pfx_base_url",
    "_raising_auth",
    "_raising_remote",
    "_remote",
    "_retry_and_raising_session",
    "_session",
    "_setup_datamart",
]


@pytest.mark.parametrize(
    "msg, results",
    [
        (msg, results)
        for msg in [None, "a message"]
        for results in [None, {"foo": 42, "bar": "baz"}]
    ],
)
def test_platform_job_should_be_able_to_update_the_state_of_the_corresponding_jst(
    _conn: ConnectionAsync,
    _job_jst: dict[str, Any],
    msg: str | None,
    results: dict[str, Any] | None,
):
    # given a PlatformJob
    job = PlatformJob(_conn, _job_jst["id"])

    # when updating its status
    job.update_status(42, msg, results)

    # then the underlying jst is updated
    jst = _conn.get_object(f"{_job_jst['id']}.JST")
    assert jst is not None

    assert "progress" in jst
    assert jst["progress"] == "42%"
    # and message and results have not been altered

    if msg is None:
        assert "messages" not in jst
    else:
        assert "messages" in jst
        assert jst["messages"] == json.dumps([msg])

    if results is None:
        assert "calculationResults" not in jst
    else:
        assert results == _calculation_results_as_dict(jst["calculationResults"])


def test_platform_job_should_be_able_to_update_progress_messages_to_the_corresponding_jst(
    _conn: ConnectionAsync,
    _job_jst: dict[str, Any],
):
    def assert_jst_has_progress_and_messages(progress: int, messages: list[str]):
        # then the underlying jst is updated
        jst = _conn.get_object(f"{_job_jst['id']}.JST")
        assert jst is not None

        assert "progress" in jst
        assert jst["progress"] == f"{progress}%"

        jst = _conn.get_object(f"{_job_jst['id']}.JST")
        assert jst is not None
        assert "messages" in jst
        assert json.loads(jst["messages"]) == messages

    # given a PlatformJob
    job = PlatformJob(_conn, _job_jst["id"])
    first_update = (42, "first message")
    second_update = (90, "second message")

    # when updating its progress
    job.update_progress(*first_update)

    # then
    assert_jst_has_progress_and_messages(first_update[0], [first_update[1]])

    # when updating a second time the progress
    job.update_progress(*second_update)

    # then
    assert_jst_has_progress_and_messages(second_update[0], [first_update[1], second_update[1]])


def test_platform_job_should_be_able_to_set_results_to_the_corresponding_jst(
    _conn: ConnectionAsync,
    _job_jst: dict[str, Any],
):
    def assert_jst_has_result(result: dict[str, Any]):
        # then the underlying jst is updated
        jst = _conn.get_object(
            f"{_job_jst['id']}.JST",
        )
        assert jst is not None
        assert "calculationResults" in jst
        assert result == _calculation_results_as_dict(jst["calculationResults"])

    # given a PlatformJob
    job = PlatformJob(_conn, _job_jst["id"])
    first_results = {"foo": 42, "bar": "baz"}
    second_results = {"spam": 12, "eggs": "sausage"}

    # when setting its results
    job.set_results(first_results)

    # then
    assert_jst_has_result(first_results)

    # when setting a second time the results
    job.set_results(second_results)

    # then
    assert_jst_has_result(second_results)


def test_model_object_should_be_able_to_create_and_read_owned_tables(
    _conn: ConnectionAsync, _model_object: dict[str, Any], tmp_path
):
    # given an instance without any model tables
    mo = ModelObject.from_conn(_conn, _model_object["typedId"])
    assert mo is not None
    assert len(list(mo.tables())) == 0

    # when pushing a table
    data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    dataframe = pd.DataFrame(data)
    table_name = "a_table_name"
    table_label = "a_table_label"

    mo.tables().push(
        table_name,
        [
            {"name": "column1", "type": "TEXT", "key": True},
            {"name": "column2", "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(dataframe),
        table_label,
        replace_existing=True,
    )

    # then it has been added to the model tables
    assert len(list(mo.tables())) == 1
    table = mo.tables()[0]
    assert table.name == table_name
    assert table.label == table_label

    typedid = table.typedid

    # and it is possible to get it from its name
    assert mo.tables().get_by_name(table_name).typedid == typedid
    assert mo.tables()[table_name].typedid == typedid

    # and it is possible to get it by typedid
    assert mo.tables().get(typedid).typedid == typedid

    # and its is possible to fetch its content as a csv stream
    downloaded_df = _csv_stream_to_dataframe(table.stream(128))
    assert (dataframe[list(data.keys())] == downloaded_df[list(data.keys())]).all().all()

    # and it is possible to fetch its content into a file
    file = tmp_path / "data.csv"
    table.to_file(file)
    saved_content = pd.read_csv(file)
    assert (dataframe[list(data.keys())] == saved_content[list(data.keys())]).all().all()

    # and it is possible to fetch its content as a pandas DataFrame
    fetched_df = table.to_pandas()
    assert fetched_df.index.name == "column1"
    assert (dataframe.set_index("column1") == fetched_df).all().all()


@pytest.mark.parametrize("fetch_method", ["to_pandas", "to_pandas_paginated"])
def test_model_object_table_should_be_filterable_with_dict_list_and_advanced_criteria(
    _conn: ConnectionAsync, _model_object: dict[str, Any], fetch_method: str
):
    # given a table with several rows, only one of which matches product=A and customer=Y
    mo = ModelObject.from_conn(_conn, _model_object["typedId"])
    data = {
        "product": ["A", "A", "B", "B"],
        "customer": ["X", "Y", "X", "Y"],
        "amount": [1, 2, 3, 4],
    }
    df = pd.DataFrame(data)
    mo.tables().push(
        "a_table",
        [
            {"name": "product", "type": "TEXT"},
            {"name": "customer", "type": "TEXT"},
            {"name": "amount", "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(df),
        "a_table_label",
        replace_existing=True,
    )
    table = mo.tables()[0]
    fetch = getattr(table, fetch_method)

    expected = df[(df["product"] == "A") & (df["customer"] == "Y")].reset_index(drop=True)

    # when filtering with a plain dict
    df_from_dict = fetch(filters={"product": "A", "customer": "Y"})

    # and with a sequence of FieldRule
    field_rules = [
        FieldRule(field_name="product", operator=FilterOperator.EQUALS, value="A"),
        FieldRule(field_name="customer", operator=FilterOperator.EQUALS, value="Y"),
    ]
    df_from_list = fetch(filters=field_rules)

    # and with an equivalent AdvancedCriteria
    df_from_advanced = fetch(filters=AdvancedCriteria(operator=Operator.AND, criteria=field_rules))

    # then all three forms return the same, correctly filtered result
    for result in (df_from_dict, df_from_list, df_from_advanced):
        assert (result.reset_index(drop=True)[list(data.keys())] == expected).all().all()


@pytest.mark.parametrize("fetch_method", ["to_pandas", "to_pandas_paginated"])
def test_model_object_table_should_be_able_to_project_a_subset_of_columns(
    _conn: ConnectionAsync, _model_object: dict[str, Any], fetch_method: str
):
    # given a table with several columns
    mo = ModelObject.from_conn(_conn, _model_object["typedId"])
    data = {
        "product": ["A", "B"],
        "customer": ["X", "Y"],
        "amount": [1, 2],
    }
    df = pd.DataFrame(data)
    mo.tables().push(
        "a_table",
        [
            {"name": "product", "type": "TEXT"},
            {"name": "customer", "type": "TEXT"},
            {"name": "amount", "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(df),
        "a_table_label",
        replace_existing=True,
    )
    table = mo.tables()[0]
    fetch = getattr(table, fetch_method)

    # when fetching with only a subset of columns
    result = fetch(columns=["product", "amount"])

    # then only the requested columns are returned, with the expected content
    assert set(result.columns) == {"product", "amount"}
    expected = df.sort_values("product").reset_index(drop=True)[["product", "amount"]]
    actual = result.sort_values("product").reset_index(drop=True)
    assert (actual == expected).all().all()


_VALID_DATAFRAME_CONTENT = [
    ([1, 2, 3, 4], lambda x: x, pd.Int64Dtype()),
    (
        [
            # pfx csv are only seconds precision
            pd.to_datetime("2020-01-02T02:02:01.000", utc=True),
            pd.to_datetime("2020-01-03T03:03:02.000", utc=True),
            None,
            pd.to_datetime("2020-01-04T04:04:03.000", utc=True),
        ],
        lambda x: pd.to_datetime(x, utc=True),
        pd.DatetimeTZDtype(tz="UTC"),
    ),
    (["foo", "bar", None, "spam"], lambda x: x, pd.StringDtype()),
    ([3.14, 12.42, None, 42.12], lambda x: x, np.float64),
    ([True, False, None, False], lambda x: x, pd.BooleanDtype()),
]


@pytest.mark.parametrize(
    "column_data, value_parser, pd_dtype",
    [(data, parser, pd_dtype) for data, parser, pd_dtype in _VALID_DATAFRAME_CONTENT],
)
def test_should_be_able_to_push_a_dataframe_to_a_model_table(
    _conn: ConnectionAsync, _model_object: dict[str, Any], column_data, value_parser, pd_dtype
):
    # given an instance without any model tables
    mo = ModelObject.from_conn(_conn, _model_object["typedId"])
    assert mo is not None
    assert len(list(mo.tables())) == 0
    columns = ["column1", "column2"]
    column_labels = {
        columns[0]: "c1 label",
        columns[1]: "c2 label",
        "index": "idx label",
    }
    # when pushing a model table
    initial_data = {
        columns[0]: pd.Series([f"key{idx + 1}" for idx in range(len(column_data))]),
        columns[1]: pd.Series(column_data, dtype=pd_dtype),
    }
    dataframe = pd.DataFrame(initial_data)
    table_name = "a_table_name"
    table_label = "a_datasource_label"

    mo.tables().push_pandas(
        table_name,
        dataframe,
        table_label,
        replace_existing=True,
        column_labels=column_labels,
    )

    # then the table is properly created
    result_table = mo.tables()[0]
    assert result_table.name == table_name
    assert result_table.label == table_label

    # and the index field label has been set (we don't check the others standard fields
    # labels as the REST API does not provide this information)
    table_dto = _conn.get_object(result_table.typedid)
    assert len(table_dto["keyFields"]) == 1
    for field in table_dto["keyFields"]:
        assert field["label"] == column_labels[field["name"]]

    # and it is possible to fetch its content as a pandas DataFrame
    fetched_df = result_table.to_pandas().sort_values(by="index").reset_index()
    fetched_df[columns[1]] = fetched_df[columns[1]].apply(value_parser)
    assert_equal_dataframes_with_na_on_cols(dataframe, fetched_df, initial_data.keys())


@pytest.mark.parametrize(
    "idx_names",
    [None, ["foo", "bar"]],
)
def test_model_object_should_be_able_to_push_and_update_multiindex_dataframes(
    _conn: ConnectionAsync, _model_object: dict[str, Any], idx_names
):
    # given an instance without any model tables
    mo = ModelObject.from_conn(_conn, _model_object["typedId"])
    assert mo is not None
    assert len(list(mo.tables())) == 0

    # when pushing a multi-index model table
    initial_data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    idx_1 = [1, 3]
    idx_2 = [2, 4]
    table_name = "a_table_name"
    original_df = pd.DataFrame(
        initial_data,
        index=pd.MultiIndex.from_tuples(zip(idx_1, idx_2), names=idx_names),
    )
    mo.tables().push_pandas(table_name, original_df)

    # then the table is properly created with index columns
    downloaded_df = list(mo.tables())[0].to_pandas()
    original_indexed = original_df.copy()
    expected_idx_names = idx_names if idx_names is not None else ["level_0", "level_1"]
    original_indexed.index.names = expected_idx_names
    assert list(downloaded_df.index.names) == expected_idx_names
    assert (original_indexed == downloaded_df).all().all()

    # when updating a multi-index model table
    updated_data = {
        "column1": ["key42", "key3"],
        "column2": [42, 0],
    }
    idx_1 = [3, 5]
    idx_2 = [4, 6]
    table_name = "a_table_name"
    updated_df = pd.DataFrame(
        updated_data,
        index=pd.MultiIndex.from_tuples(zip(idx_1, idx_2), names=idx_names),
    )
    list(mo.tables())[0].update_pandas(updated_df)

    # then the table is properly updated
    downloaded_df = list(mo.tables())[0].to_pandas().sort_index().reset_index()
    # when updating a multi-index model table
    expected_data = {
        "column1": ["key1", "key42", "key3"],
        "column2": [1, 42, 0],
    }
    idx_1 = [1, 3, 5]
    idx_2 = [2, 4, 6]
    table_name = "a_table_name"
    expected_df = pd.DataFrame(
        expected_data,
        index=pd.MultiIndex.from_tuples(zip(idx_1, idx_2), names=idx_names),
    ).reset_index(drop=False)
    assert (expected_df == downloaded_df).all().all()


def test_model_object_should_be_able_to_update_data_of_an_owned_tables(
    _conn: ConnectionAsync, _model_object: dict[str, Any]
):
    # given an instance without an owned tables
    mo = ModelObject.from_conn(_conn, _model_object["typedId"])
    initial_data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    index = [1, 2]
    initial_df = pd.DataFrame(initial_data, index=index)
    table_name = "a_table_name"
    mo.tables().push_pandas(
        table_name,
        initial_df,
        replace_existing=True,
    )
    table = list(mo.tables())[0]

    # when appending new data
    new_data = {
        "index": [2, 3],
        "column1": ["key22", "key3"],
        "column2": [0, 24],
    }
    new_df = pd.DataFrame(new_data)
    table.update(AvroStream.from_dataframe(new_df))

    # then the new content is added it is possible to get it
    expected_data = {
        "index": [1, 2, 3],
        "column1": ["key1", "key22", "key3"],
        "column2": [1, 0, 24],
    }
    expected_df = pd.DataFrame(expected_data)
    downloaded_df = table.to_pandas().sort_values(by="index").reset_index()
    assert (expected_df[initial_df.columns] == downloaded_df[initial_df.columns]).all().all()


@pytest.mark.parametrize(
    "column_data, value_parser, pd_dtype",
    [(data, parser, pd_dtype) for data, parser, pd_dtype in _VALID_DATAFRAME_CONTENT],
)
def test_model_object_should_be_able_to_update_data_of_an_owned_tables_from_pandas(
    _conn: ConnectionAsync, _model_object: dict[str, Any], column_data, value_parser, pd_dtype
):
    # given an instance without an owned tables
    mo = ModelObject.from_conn(_conn, _model_object["typedId"])
    index = [1, 2]
    initial_data = {
        "column1": [f"key{idx + 1}" for idx in range(0, 2)],
        "column2": pd.Series(column_data[:2], dtype=pd_dtype, index=index),
    }
    initial_df = pd.DataFrame(initial_data, index=index)
    table_name = "a_table_name"
    mo.tables().push_pandas(
        table_name,
        initial_df,
        replace_existing=True,
    )
    table = list(mo.tables())[0]

    # when appending new data
    index = [2, 3]
    new_data = {
        "column1": [f"key{idx + 1}" for idx in range(1, 3)],
        "column2": pd.Series([column_data[3], column_data[2]], dtype=pd_dtype, index=index),
    }
    new_df = pd.DataFrame(new_data, index=index)
    table.update_pandas(new_df)

    # then the new content is added it is possible to get it
    index = [1, 2, 3]
    expected_data = {
        "column1": [f"key{idx + 1}" for idx in range(0, 3)],
        "column2": pd.Series(
            [column_data[0], column_data[3], column_data[2]], dtype=pd_dtype, index=index
        ),
    }
    expected_df = pd.DataFrame(expected_data, index=index)
    downloaded_df = table.to_pandas().sort_values(by="index").reset_index()
    downloaded_df["column2"] = downloaded_df["column2"].apply(value_parser)
    assert_equal_dataframes_with_na_on_cols(
        expected_df.reset_index(), downloaded_df, initial_data.keys()
    )


def test_model_object_should_be_able_to_create_and_read_an_attachment(
    _conn: ConnectionAsync,
    _model_object: dict[str, Any],
):
    # when getting an empty model object
    mo = ModelObject.from_conn(_conn, _model_object["typedId"])

    # then the model does not have any attachment
    assert mo is not None
    assert len(list(mo.attachments())) == 0

    # when pushing an attachment
    attachment_name = "anAttachmentName"
    attachment_content = "the attachment content\nwith a second line\nan a last line"
    mo.attachments().push(attachment_name, StringIO(attachment_content))

    # then it has been added to the model's attachments
    assert len(list(mo.attachments())) == 1
    attachment = mo.attachments()[0]
    assert attachment.name == attachment_name

    # then it is possible to get it from its name
    assert mo.attachments().get_by_name(attachment_name).typedid == attachment.typedid
    assert mo.attachments()[attachment_name].typedid == attachment.typedid

    # and its is possible to fetch its content as a stream
    data = next(attachment.download_file()).decode("utf-8")
    assert data == attachment_content


def test_should_be_able_to_create_and_read_datasources(_partition: Partition, tmp_path):
    # given an instance without any datasource
    datasources = _partition.datasources()
    ds_list = list(datasources)
    assert len(ds_list) == 0

    # when pushing a data source
    data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    col_names = list(data.keys())
    dataframe = pd.DataFrame(data)
    data_source_name = "a_datasource_name"
    data_source_label = "a_datasource_label"

    datasources.push(
        data_source_name,
        [
            {"name": col_names[0], "type": "TEXT", "key": True},
            {"name": col_names[1], "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(dataframe),
        data_source_label,
        replace_existing=True,
    )

    # then it should be listed by DataSources
    ds_list = list(datasources)
    assert len(ds_list) == 1
    datasource = ds_list[0]
    assert datasource.name == data_source_name
    assert datasource.label == data_source_label

    # and datasources is able to get it by name
    assert datasources.get_by_name(data_source_name).typedid == datasource.typedid
    assert datasources[data_source_name].typedid == datasource.typedid

    # and datasources is able to get it by typedid
    assert datasources.get(datasource.typedid).typedid == datasource.typedid

    # and its is possible to fetch its content as a csv stream
    df_from_stream = _csv_stream_to_dataframe(datasource.stream(128))
    assert (dataframe[col_names] == df_from_stream[col_names]).all().all()

    # and it is possible to fetch its content into a file
    file = tmp_path / "data.csv"
    datasource.to_file(file)
    saved_content = pd.read_csv(file)
    assert (dataframe[list(data.keys())] == saved_content[list(data.keys())]).all().all()

    # and it is possible to fetch its content as a pandas DataFrame
    fetched_df = datasource.to_pandas()
    assert fetched_df.index.name == col_names[0]
    assert (dataframe.set_index(col_names[0]) == fetched_df[col_names[1:]]).all().all()


@pytest.mark.parametrize(
    "column_data, value_parser, pd_dtype",
    [(data, parser, pd_dtype) for data, parser, pd_dtype in _VALID_DATAFRAME_CONTENT],
)
def test_should_be_able_to_push_a_dataframe_to_a_datasource(
    _partition: Partition,
    column_data,
    value_parser,
    pd_dtype,
    _conn,
):
    # given an instance without any datasource
    assert len(list(_partition.datasources())) == 0
    columns = ["column1", "column2"]
    column_labels = {
        columns[0]: "c1 label",
        columns[1]: "c2 label",
        "index": "idx label",
    }
    # when pushing a data source
    initial_data = {
        columns[0]: pd.Series([f"key{idx + 1}" for idx in range(len(column_data))]),
        columns[1]: pd.Series(column_data, dtype=pd_dtype),
    }
    dataframe = pd.DataFrame(initial_data)
    data_source_name = "a_datasource_name"
    data_source_label = "a_datasource_label"

    _partition.datasources().push_pandas(
        data_source_name,
        dataframe,
        data_source_label,
        replace_existing=True,
        column_labels=column_labels,
    )

    # then the table is properly created
    result_table = _partition.datasources()[0]
    assert result_table.name == data_source_name
    assert result_table.label == data_source_label

    # and the index field label has been set (we don't check the others standard fields
    # labels as the REST API does not provide this information)
    table_dto = _conn.get_object(result_table.typedid)
    assert len(table_dto["keyFields"]) == 1
    for field in table_dto["keyFields"]:
        assert field["label"] == column_labels[field["name"]]

    # and it is possible to fetch its content as a pandas DataFrame
    fetched_df = result_table.to_pandas().sort_values(by="index").reset_index()
    fetched_df[columns[1]] = fetched_df[columns[1]].apply(value_parser)
    assert_equal_dataframes_with_na_on_cols(dataframe, fetched_df, initial_data.keys())


@pytest.mark.parametrize("column_data, value_parser, pd_dtype", _VALID_DATAFRAME_CONTENT)
def test_should_be_able_to_update_data_of_a_data_source_tables(
    _partition: Partition, column_data, value_parser, pd_dtype
):
    # IMPORTANT NOTE:
    # We *cannot* properly test updating existing rows, as the row deduplication process is
    # asynchronous and may only trigger after a significant delay.
    #
    # So for DS we only test for appending new rows.

    # given an instance without an owned tables
    index = [1, 2]
    initial_data = {
        "column1": [f"key{idx + 1}" for idx in range(0, 2)],
        "column2": pd.Series(column_data[:2], dtype=pd_dtype, index=index),
    }
    initial_df = pd.DataFrame(initial_data, index=index)
    data_source_name = "a_datasource_name"
    data_source_label = "a_datasource_label"
    _partition.datasources().push_pandas(
        data_source_name,
        initial_df,
        data_source_label,
        replace_existing=True,
    )
    table = list(_partition.datasources())[0]

    # when update new data
    index = [3]
    new_data = {
        "column1": [f"key{idx + 1}" for idx in range(2, 3)],
        "column2": pd.Series([column_data[2]], dtype=pd_dtype, index=index),
    }
    new_df = pd.DataFrame(new_data, index=index)
    table.update(AvroStream.from_dataframe(new_df.reset_index(drop=False)))

    # then the new content is added it is possible to get it
    expected_data = {
        "column1": [f"key{idx + 1}" for idx in range(0, 3)],
        "column2": pd.Series(column_data[:3], dtype=pd_dtype),
    }
    expected_df = pd.DataFrame(expected_data)
    downloaded_df = table.to_pandas().sort_values(by="index").reset_index()
    downloaded_df["column2"] = downloaded_df["column2"].apply(value_parser)
    assert_equal_dataframes_with_na_on_cols(expected_df, downloaded_df, initial_data.keys())


@pytest.mark.parametrize("column_data, value_parser, pd_dtype", _VALID_DATAFRAME_CONTENT)
def test_should_be_able_to_update_data_of_a_data_source_tables_from_pandas(
    _partition: Partition, column_data, value_parser, pd_dtype
):
    # IMPORTANT NOTE:
    # We *cannot* properly test updating existing rows, as the row deduplication process is
    # asynchronous and may only trigger after a significant delay.
    #
    # So for DS we only test for appending new rows.

    # given an instance without an owned tables
    index = [1, 2]
    initial_data = {
        "column1": [f"key{idx + 1}" for idx in range(0, 2)],
        "column2": pd.Series(column_data[:2], dtype=pd_dtype, index=index),
    }
    initial_df = pd.DataFrame(initial_data, index=index)
    data_source_name = "a_datasource_name"
    data_source_label = "a_datasource_label"
    _partition.datasources().push_pandas(
        data_source_name,
        initial_df,
        data_source_label,
        replace_existing=True,
    )
    table = list(_partition.datasources())[0]

    # when appending new data
    index = [3]
    new_data = {
        "column1": [f"key{idx + 1}" for idx in range(2, 3)],
        "column2": pd.Series([column_data[2]], dtype=pd_dtype, index=index),
    }
    new_df = pd.DataFrame(new_data, index=index)
    table.update_pandas(new_df)

    # then the new content is added it is possible to get it
    index = [1, 2, 3]
    expected_data = {
        "column1": [f"key{idx + 1}" for idx in range(0, 3)],
        "column2": pd.Series(column_data[:3], dtype=pd_dtype),
    }
    expected_df = pd.DataFrame(expected_data)
    downloaded_df = table.to_pandas().sort_values(by="index").reset_index()
    downloaded_df["column2"] = downloaded_df["column2"].apply(value_parser)
    assert_equal_dataframes_with_na_on_cols(expected_df, downloaded_df, initial_data.keys())


def assert_equal_dataframes_with_na_on_cols(
    dataframe1: pd.DataFrame, dataframe2: pd.DataFrame, cols: Iterable[str]
):
    assert (
        (
            (dataframe1[cols] == dataframe2[cols]).fillna(False)
            | (dataframe1[cols].isna() & dataframe2[cols].isna())
        )
        .all()
        .all()
    )


def test_should_be_able_to_create_and_read_datamarts(
    _setup_datamart: tuple[Partition, str, list[str]], tmp_path: str
):
    _partition, dm_name, col_names = _setup_datamart

    # then it has been added to the datamarts
    assert len(list(_partition.datamarts())) == 1
    datamart = _partition.datamarts()[0]
    assert datamart.name == dm_name

    # and it is possible to get it from its name
    assert _partition.datamarts().get_by_name(dm_name).typedid == datamart.typedid
    assert _partition.datamarts()[dm_name].typedid == datamart.typedid

    # and it is possible to get it by typedid
    assert _partition.datamarts().get(datamart.typedid).typedid == datamart.typedid

    # and its is possible to fetch its content as a csv stream
    data = next(datamart.stream(128)).decode("utf-8")
    assert ",".join(col_names) in data

    # and it is possible to fetch its content into a file
    file = tmp_path / "data.csv"
    datamart.to_file(file)
    data = file.read_text("utf-8")
    assert ",".join(col_names) in data

    # and it is possible to fetch its content as a pandas DataFrame
    data_frame = _partition.datamarts()[dm_name].to_pandas()
    assert data_frame.shape == (0, 8)
    assert data_frame.index.name == col_names[0]
    assert all(col in data_frame.columns for col in col_names[1:])


def test_should_give_access_to_model_objects(_partition: Partition, _model_object: dict[str, Any]):
    # A model object has been added by _model_object
    mo_name = _model_object["uniqueName"]
    model_objects = list(_partition.model_objects())
    assert len(model_objects) == 1
    modelobject = model_objects[0]
    assert modelobject.name == mo_name

    # and it is possible to get it from its name
    assert _partition.model_objects().get_by_name(mo_name).typedid == modelobject.typedid
    assert _partition.model_objects()[mo_name].typedid == modelobject.typedid

    # and it is possible to get it by typedId
    assert _partition.model_objects().get(modelobject.typedid).typedid == modelobject.typedid


@pytest.mark.parametrize(
    "column_data, value_parser, pd_dtype",
    [(data, parser, pd_dtype) for data, parser, pd_dtype in _VALID_DATAFRAME_CONTENT],
)
def test_should_retry_to_push_a_dataframe_to_a_model_table(
    _connection_with_raising_session: tuple[ConnectionAsync, _RaisingExceptionSession],
    _model_object: dict[str, Any],
    column_data,
    value_parser,
    pd_dtype,
):
    _conn, raising_session = _connection_with_raising_session
    # given an instance without any model tables
    mo = ModelObject.from_conn(_conn, _model_object["typedId"])
    assert mo is not None
    assert len(list(mo.tables())) == 0
    columns = ["column1", "column2"]
    column_labels = {
        columns[0]: "c1 label",
        columns[1]: "c2 label",
        "index": "idx label",
    }
    # when pushing a model table
    initial_data = {
        columns[0]: pd.Series([f"key{idx + 1}" for idx in range(len(column_data))]),
        columns[1]: pd.Series(column_data, dtype=pd_dtype),
    }
    dataframe = pd.DataFrame(initial_data)
    table_name = "a_table_name"
    table_label = "a_datasource_label"

    raising_session.reset_counter()
    mo.tables().push_pandas(
        table_name,
        dataframe,
        table_label,
        replace_existing=True,
        column_labels=column_labels,
    )
    assert raising_session._counter == raising_session._retries

    # then the table is properly created
    result_table = mo.tables()[0]
    assert result_table.name == table_name
    assert result_table.label == table_label

    # and the index field label has been set (we don't check the others standard fields
    # labels as the REST API does not provide this information)
    table_dto = _conn.get_object(result_table.typedid)
    assert len(table_dto["keyFields"]) == 1
    assert all(field["label"] == column_labels[field["name"]] for field in table_dto["keyFields"])

    # and it is possible to fetch its content as a pandas DataFrame
    fetched_df = result_table.to_pandas().sort_values(by="index").reset_index()
    fetched_df[columns[1]] = fetched_df[columns[1]].apply(value_parser)
    assert_equal_dataframes_with_na_on_cols(dataframe, fetched_df, initial_data.keys())


@pytest.mark.parametrize(
    "column_data, value_parser, pd_dtype",
    [(data, parser, pd_dtype) for data, parser, pd_dtype in _VALID_DATAFRAME_CONTENT],
)
def test_model_object_should_retry_to_update_data_of_an_owned_tables_from_pandas(
    _connection_with_raising_session: tuple[ConnectionAsync, _RaisingExceptionSession],
    _model_object: dict[str, Any],
    column_data,
    value_parser,
    pd_dtype,
):
    _conn, raising_session = _connection_with_raising_session
    # given an instance without an owned tables
    mo = ModelObject.from_conn(_conn, _model_object["typedId"])
    index = [1, 2]
    initial_data = {
        "column1": [f"key{idx + 1}" for idx in range(0, 2)],
        "column2": pd.Series(column_data[:2], dtype=pd_dtype, index=index),
    }
    initial_df = pd.DataFrame(initial_data, index=index)
    table_name = "a_table_name"
    mo.tables().push_pandas(
        table_name,
        initial_df,
        replace_existing=True,
    )
    table = list(mo.tables())[0]

    # when appending new data
    index = [2, 3]
    new_data = {
        "column1": [f"key{idx + 1}" for idx in range(1, 3)],
        "column2": pd.Series([column_data[3], column_data[2]], dtype=pd_dtype, index=index),
    }
    new_df = pd.DataFrame(new_data, index=index)
    raising_session.reset_counter()
    table.update_pandas(new_df)
    assert raising_session._counter == raising_session._retries

    # then the new content is added it is possible to get it
    index = [1, 2, 3]
    expected_data = {
        "column1": [f"key{idx + 1}" for idx in range(0, 3)],
        "column2": pd.Series(
            [column_data[0], column_data[3], column_data[2]], dtype=pd_dtype, index=index
        ),
    }
    expected_df = pd.DataFrame(expected_data, index=index)
    downloaded_df = table.to_pandas().sort_values(by="index").reset_index()
    downloaded_df["column2"] = downloaded_df["column2"].apply(value_parser)
    assert_equal_dataframes_with_na_on_cols(
        expected_df.reset_index(), downloaded_df, initial_data.keys()
    )


def test_push_pandas_should_be_able_to_define_field_types(
    _conn: ConnectionAsync,
    _partition: Partition,
):
    # given an instance without any datasource
    assert len(list(_partition.datasources())) == 0

    data = [
        [
            "2023-04-01",
            "2023-04-01T00:00:00",
            "P-0058",
            58.2988557924782,
            108,
            "CZK",
            1608.07090971086,
            108,
            True,
            "inch",
        ],
        [
            "2023-05-01",
            "2023-05-01T00:00:00",
            "P-0058",
            58.2988551055411,
            109,
            "CZK",
            1650.94815814065,
            109,
            False,
            "inch",
        ],
        [
            "2023-04-01",
            "2023-04-01T00:00:00",
            "P-0135",
            87.4740059147734,
            256,
            "CZK",
            3857.3131275826,
            256,
            True,
            "inch",
        ],
        [
            "2023-05-01",
            "2023-05-01T00:00:00",
            "P-0135",
            86.5278991416824,
            257,
            "CZK",
            4411.62163412218,
            257,
            False,
            "inch",
        ],
        [
            "2023-04-01",
            "2023-04-01T00:00:00",
            "P-0229",
            42.9735533892808,
            430,
            "CZK",
            4059.66661853271,
            430,
            True,
            "inch",
        ],
        [
            "2023-05-01",
            "2023-05-01T00:00:00",
            "P-0229",
            42.9740376458877,
            431,
            "CZK",
            4386.04444857636,
            431,
            False,
            "inch",
        ],
        [
            "2023-04-01",
            "2023-04-01T00:00:00",
            "P-0341",
            145.988427286104,
            622,
            "CZK",
            7590.28918414268,
            622,
            True,
            "inch",
        ],
        [
            "2023-05-01",
            "2023-05-01T00:00:00",
            "P-0341",
            145.988429620856,
            623,
            "CZK",
            7955.92914648325,
            623,
            False,
            "inch",
        ],
    ]
    columns = [
        "date",
        "datetime",
        "text",
        "number",
        "integer",
        "currency",
        "money",
        "quantity",
        "boolean",
        "UOM",
    ]

    dataframe = pd.DataFrame(data, columns=columns, dtype="object")
    table_name = "TableWithManualSpecs"
    dataframe_specs = FieldSpecs()
    for col in columns:
        dataframe_specs.set_col_specs(col, type=col.upper())

    _partition.datasources().push_pandas(table_name, dataframe, manual_fields_specs=dataframe_specs)

    datasources_meta = _conn.list_fcs("DMDS")
    pushed_table = next(
        (table_def for table_def in datasources_meta if table_def["uniqueName"] == table_name), {}
    )
    assert len(pushed_table) > 0

    fields_remapped = {}
    for field in pushed_table["fields"]:
        fields_remapped[field["name"]] = field

    fields_remapped = {field["name"]: field for field in pushed_table["fields"]}
    assert all(fields_remapped[col]["type"] == col.upper() for col in columns)


def test_push_pandas_should_support_lob_field_with_long_text(
    _conn: ConnectionAsync,
    _partition: Partition,
):
    # given an instance without any datasource
    assert len(list(_partition.datasources())) == 0

    # when pushing a datasource with a manually-declared LOB column holding >255 chars.
    # LOB push requires the pricefx-core AvroData gate fix (accept a STRING avro payload for
    # a LOB field), available from core 16.3.13 / 17.0.4 up. On older backends pyfxlib's own
    # version gate raises RuntimeError before sending; from 16.3.13 / 17.0.4 onwards the push
    # succeeds.
    long_text = "x" * 1000
    dataframe = pd.DataFrame({"key": ["k1"], "bigtext": [long_text]}).set_index("key")
    specs = FieldSpecs()
    specs.set_col_specs("bigtext", type="LOB")

    _partition.datasources().push_pandas("lob_datasource", dataframe, manual_fields_specs=specs)

    # then the field is declared as LOB
    table = _partition.datasources()[0]
    bigtext_field = next(field for field in table.fields if field["name"] == "bigtext")
    assert bigtext_field["type"] == "LOB"

    # and the >255 char value round-trips intact
    assert table.to_pandas()["bigtext"].iloc[0] == long_text

    # and appending another LOB row via update_pandas works (plain-string payload accepted)
    other_text = "y" * 2000
    append_df = pd.DataFrame({"key": ["k2"], "bigtext": [other_text]}).set_index("key")
    table.update_pandas(append_df)

    fetched = table.to_pandas().sort_index()
    assert fetched.loc["k1", "bigtext"] == long_text
    assert fetched.loc["k2", "bigtext"] == other_text
