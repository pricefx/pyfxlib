import json
import time
from io import StringIO
from typing import Any, Dict

import pandas as pd

import pytest

from requests import HTTPError

from pyfxlib._testtooling.conftest import (
    _auth,
    _conn,
    _model_object,
    _pfx_base_url,
    _remote,
    _session,
)
from pyfxlib._testtooling.helpers import (
    _IntegrationRemote,
    _calculation_results_as_dict,
    _csv_stream_to_dataframe,
)
from pyfxlib.lowlevel.avro import AvroStream
from pyfxlib.lowlevel.connection import Connection
from pyfxlib.lowlevel.connection import JobStatus

__all__ = ["_auth", "_conn", "_model_object", "_pfx_base_url", "_remote", "_session"]


def assert_are_equal_for_common_dict_keys(dict_1: Dict[str, Any], dict_2: Dict[str, Any]) -> None:
    common_keys = set(dict_1.keys()).intersection(dict_2.keys())

    def common_key_dict(dic: Dict[str, Any]) -> Dict[str, Any]:
        return {key: value for key, value in dic.items() if key in common_keys}

    assert len(common_keys) > 0, "at least one common key required, none found"
    assert common_key_dict(dict_1) == common_key_dict(dict_2)


def test_connection_should_be_able_to_add_an_object_update_it_list_it_and_fetch_it(
    _conn: Connection,
):
    # when adding a product
    original_label = "aProductlabel"
    updated_label = "anotherProductlabel"
    res = _conn.add_object("P", {"label": original_label, "sku": "the_sku"})

    # then the returned object contains at least the typedId and the given attributes
    assert "typedId" in res.keys()
    assert res["label"] == original_label

    # when updating this object with a new uniqueName
    res["label"] = updated_label
    updated_res = _conn.update_object("P", res)

    # then the returned object is containing the updated version and attributes values
    assert updated_res["label"] == updated_label
    assert updated_res["version"] == res["version"] + 1

    # when getting the list of products
    products = _conn.list_objects("P")

    # then only one exists and updated
    assert len(products) == 1
    assert_are_equal_for_common_dict_keys(products[0], updated_res)

    # and it can be fetched with get_object
    fetch_pl = _conn.get_object(res["typedId"])
    assert_are_equal_for_common_dict_keys(fetch_pl, updated_res)


def test_push_pull_and_list_data_source_should_work_as_expected(_conn: Connection):
    # when pushing a data source
    data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    dataframe = pd.DataFrame(data)

    data_source_name = "sample_data_source"
    data_source_label = "sample_data_source_label"
    _conn.create_table(
        data_source_name,
        [
            {"name": "column1", "type": "TEXT", "key": True},
            {"name": "column2", "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(dataframe),
        data_source_label,
        replace_existing=True,
    )

    # then it can be listed as DMDS via list_fcs
    fcs = _conn.list_fcs("DMDS")
    assert len(fcs) == 1
    assert fcs[0]["label"] == data_source_label
    assert fcs[0]["uniqueName"] == data_source_name

    # and it can be fetched using its typedId via get_fcs
    data_souce = _conn.get_fcs(fcs[0]["typedId"])
    assert data_souce["label"] == data_source_label
    assert data_souce["uniqueName"] == data_source_name

    # and it can be listed as DMDS via list_objects
    data_souces = _conn.list_objects("DMDS")
    assert len(data_souces) == 1
    assert data_souces[0]["label"] == data_source_label
    assert data_souces[0]["uniqueName"] == data_source_name

    # and it can be pulled with stream_datasource with same content as initial pushed one
    downloaded_content = _csv_stream_to_dataframe(_conn.stream_fcs(data_souces[0]["typedId"]))
    assert (dataframe[list(data.keys())] == downloaded_content[list(data.keys())]).all().all()


def test_should_be_able_to_update_a_data_source(_conn: Connection):
    # IMPORTANT NOTE:
    # We *cannot* properly test updating existing rows, as the row deduplication process is
    # asynchronous and may only trigger after a significant delay.
    #
    # So for DS we only test for appending new rows.

    # when pushing a data source
    initial_data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    dataframe_init = pd.DataFrame(initial_data, index=[1, 2])

    data_source_name = "sample_data_source"
    data_source_label = "sample_data_source_label"
    _conn.create_table(
        data_source_name,
        [
            {"name": "column1", "type": "TEXT", "key": True},
            {"name": "column2", "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(dataframe_init),
        data_source_label,
        replace_existing=True,
    )
    ds_typedid = _conn.list_fcs("DMDS")[0]["typedId"]

    # when update new data
    new_data = {
        "column1": ["key3", "key4"],
        "column2": [2, 24],
    }
    dataframe_append = pd.DataFrame(new_data, index=[3, 4])
    _conn.update_table(ds_typedid, AvroStream.from_dataframe(dataframe_append))

    # then the new content is added to the table
    expected_data = {
        "column1": ["key1", "key2", "key3", "key4"],
        "column2": [1, 12, 2, 24],
    }
    dataframe_expected = pd.DataFrame(expected_data)
    dataframe_downloaded = _csv_stream_to_dataframe(_conn.stream_fcs(ds_typedid, 128))
    assert (dataframe_expected == dataframe_downloaded[list(dataframe_expected.keys())]).all().all()


def test_should_fail_to_update_a_data_source_when_duplicates(_conn: Connection):
    # IMPORTANT NOTE:
    # We *cannot* properly test updating existing rows, as the row deduplication process is
    # asynchronous and may only trigger after a significant delay.
    #
    # So for DS we only test for appending new rows.

    # when pushing a data source
    initial_data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    dataframe_init = pd.DataFrame(initial_data, index=[1, 2])

    data_source_name = "sample_data_source"
    data_source_label = "sample_data_source_label"
    _conn.create_table(
        data_source_name,
        [
            {"name": "column1", "type": "TEXT", "key": True},
            {"name": "column2", "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(dataframe_init),
        data_source_label,
        replace_existing=True,
    )
    ds_typedid = _conn.list_fcs("DMDS")[0]["typedId"]

    # when update new data
    new_data = {
        "column1": ["key3", "key4", "key3"],
        "column2": [2, 24, 3],
    }
    dataframe_append = pd.DataFrame(new_data, index=[1, 2, 3])

    with pytest.raises(Exception, match="Error while uploading file:"):
        _conn.update_table(ds_typedid, AvroStream.from_dataframe(dataframe_append))

    # then no new content is added to the table
    expected_data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    dataframe_expected = pd.DataFrame(expected_data)
    dataframe_downloaded = _csv_stream_to_dataframe(_conn.stream_fcs(ds_typedid, 128))
    assert (dataframe_expected == dataframe_downloaded[list(dataframe_expected.keys())]).all().all()


def test_should_be_able_to_update_the_existing_rows_on_a_data_source(_conn: Connection):
    # IMPORTANT NOTE:
    # We *cannot* properly test updating existing rows, as the row deduplication process is
    # asynchronous and may only trigger after a significant delay.
    #
    # So for DS we only test for appending new rows.

    # when pushing a data source
    initial_data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    dataframe_init = pd.DataFrame(initial_data, index=[1, 2])

    data_source_name = "sample_data_source"
    data_source_label = "sample_data_source_label"
    _conn.create_table(
        data_source_name,
        [
            {"name": "column1", "type": "TEXT", "key": True},
            {"name": "column2", "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(dataframe_init),
        data_source_label,
        replace_existing=True,
    )
    ds_typedid = _conn.list_fcs("DMDS")[0]["typedId"]

    # when update new data
    new_data = {
        "column1": ["key1", "key2"],
        "column2": [2, 24],
    }
    dataframe_append = pd.DataFrame(new_data, index=[1, 2])
    _conn.update_table(ds_typedid, AvroStream.from_dataframe(dataframe_append))

    # then the new content is added to the table
    expected_data = {
        "column1": ["key1", "key2"],
        "column2": [2, 24],
    }
    dataframe_expected = pd.DataFrame(expected_data)
    dataframe_downloaded = _csv_stream_to_dataframe(_conn.stream_fcs(ds_typedid, 128))
    assert (dataframe_expected == dataframe_downloaded[list(dataframe_expected.keys())]).all().all()


def test_should_be_able_to_attach_a_file_list_attachments_and_pull_an_attachment(
    _conn: Connection, _model_object: Dict[str, Any]
):
    # given an empty model object and an attachment content
    mo_typedid = _model_object["typedId"]
    attachment_name = "anAttachmentName"
    attachment_content = "a first line\n" "a second line\n" "the last line"

    # when attaching a file to the model object
    _conn.attach_file(mo_typedid, attachment_name, StringIO(attachment_content))

    # then this file should be present in the list of its attachments
    attachments = _conn.list_attachments(mo_typedid)
    assert len(attachments) == 1
    assert attachments[0]["fileName"] == attachment_name

    # and it is possible to fetch back its content
    content = _conn.pull_file(mo_typedid, attachments[0]["typedId"], 128)
    assert attachment_content == next(content).decode("utf-8")


def test_should_be_able_to_push_an_owned_table_and_read_it_back(
    _conn: Connection, _model_object: Dict[str, Any]
):
    # given an empty model
    mo_typedid = _model_object["typedId"]
    table_name = "sample_table_source"
    table_label = "sample_table_label"
    data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    dataframe = pd.DataFrame(data)

    # when pushing an owned table
    _conn.create_table(
        table_name,
        [
            {"name": "column1", "type": "TEXT", "key": True},
            {"name": "column2", "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(dataframe),
        table_label,
        mo_typedid,
        replace_existing=True,
    )

    # then the table is created
    fcs = _conn.list_fcs("DMT", {"owner": mo_typedid})
    assert len(fcs) == 1
    assert fcs[0]["name"] == table_name
    assert fcs[0]["label"] == table_label
    assert [x["name"] for x in fcs[0]["fields"]] == list(data.keys())

    # and we can fetch its content
    downloaded_content = _csv_stream_to_dataframe(_conn.stream_fcs(fcs[0]["typedId"], 128))
    assert (dataframe[list(data.keys())] == downloaded_content[list(data.keys())]).all().all()


def test_should_be_able_to_update_model_table(_conn: Connection, _model_object: Dict[str, Any]):
    # given a model
    mo_typedid = _model_object["typedId"]
    table_name = "sample_table_source"
    table_label = "sample_table_label"
    initial_data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    dataframe_init = pd.DataFrame(initial_data)

    # with an owned table
    _conn.create_table(
        table_name,
        [
            {"name": "column1", "type": "TEXT", "key": True},
            {"name": "column2", "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(dataframe_init),
        table_label,
        mo_typedid,
        replace_existing=True,
    )
    table_typedid = _conn.list_fcs("DMT", {"owner": mo_typedid})[0]["typedId"]

    # when appending new data
    index = [2, 3]
    new_data = {
        "column1": ["key2", "key3"],
        "column2": [42, 24],
    }
    dataframe_append = pd.DataFrame(new_data, index=index)
    _conn.update_table(table_typedid, AvroStream.from_dataframe(dataframe_append))

    # then the new content is added to the table
    expected_data = {
        "column1": ["key1", "key2", "key3"],
        "column2": [1, 42, 24],
    }
    dataframe_expected = pd.DataFrame(expected_data)
    dataframe_downloaded = _csv_stream_to_dataframe(_conn.stream_fcs(table_typedid, 128))
    assert (dataframe_expected == dataframe_downloaded[list(dataframe_expected.keys())]).all().all()


def test_should_fail_to_update_model_table_when_duplicates(
    _conn: Connection, _model_object: Dict[str, Any]
):
    # given a model
    mo_typedid = _model_object["typedId"]
    table_name = "sample_table_source"
    table_label = "sample_table_label"
    initial_data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    dataframe_init = pd.DataFrame(initial_data)

    # with an owned table
    _conn.create_table(
        table_name,
        [
            {"name": "column1", "type": "TEXT", "key": True},
            {"name": "column2", "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(dataframe_init),
        table_label,
        mo_typedid,
        replace_existing=True,
    )
    table_typedid = _conn.list_fcs("DMT", {"owner": mo_typedid})[0]["typedId"]

    # when appending new data
    index = [1, 2, 3]
    new_data = {
        "column1": ["key3", "key4", "key3"],
        "column2": [42, 24, 16],
    }
    dataframe_append = pd.DataFrame(new_data, index=index)

    with pytest.raises(Exception, match="Error while uploading file:"):
        _conn.update_table(table_typedid, AvroStream.from_dataframe(dataframe_append))

    # then the no new content is added to the table
    expected_data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    dataframe_expected = pd.DataFrame(expected_data)
    dataframe_downloaded = _csv_stream_to_dataframe(_conn.stream_fcs(table_typedid, 128))
    assert (dataframe_expected == dataframe_downloaded[list(dataframe_expected.keys())]).all().all()


def test_should_be_able_update_the_existing_rows_on_a_model_table(
    _conn: Connection, _model_object: Dict[str, Any]
):
    # given a model
    mo_typedid = _model_object["typedId"]
    table_name = "sample_table_source"
    table_label = "sample_table_label"
    initial_data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    dataframe_init = pd.DataFrame(initial_data)

    # with an owned table
    _conn.create_table(
        table_name,
        [
            {"name": "column1", "type": "TEXT", "key": True},
            {"name": "column2", "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(dataframe_init),
        table_label,
        mo_typedid,
        replace_existing=True,
    )
    table_typedid = _conn.list_fcs("DMT", {"owner": mo_typedid})[0]["typedId"]

    # when appending new data
    index = [1, 2]
    new_data = {
        "column1": ["key1", "key2"],
        "column2": [2, 24],
    }
    dataframe_append = pd.DataFrame(new_data, index=index)
    _conn.update_table(table_typedid, AvroStream.from_dataframe(dataframe_append))

    # then the new content is added to the table
    expected_data = {
        "column1": ["key1", "key2"],
        "column2": [2, 24],
    }
    dataframe_expected = pd.DataFrame(expected_data)
    dataframe_downloaded = _csv_stream_to_dataframe(_conn.stream_fcs(table_typedid, 128))
    assert (dataframe_expected == dataframe_downloaded[list(dataframe_expected.keys())]).all().all()


def test_should_be_able_to_update_a_job_status(
    _remote: _IntegrationRemote, _conn: Connection, _model_object: Dict[str, Any]
):
    # given an empty model object and an emulated job
    _remote.trigger_job(_model_object)
    jobs = _remote.jobs(_model_object["typedId"])
    assert len(jobs) == 1
    job = jobs[0]
    calc_results = {"foo": 12, "bar": "baz"}
    message = "almost finished"
    # when updating the status of the job
    _conn.update_status(
        job["id"],
        JobStatus.PROCESSING,
        42,
        message,
        calc_results,
    )

    # the JST has been updated in a proper way
    updated_job = _conn.get_object(f"{job['id']}.JST")
    assert updated_job is not None
    assert "status" in updated_job
    assert updated_job["status"] == "PROCESSING"
    assert "progress" in updated_job
    assert updated_job["progress"] == "42%"
    assert "messages" in updated_job
    assert json.loads(updated_job["messages"]) == [message]
    assert "calculationResults" in updated_job
    assert calc_results == _calculation_results_as_dict(updated_job["calculationResults"])

    # and when the job is updated with the finished status
    _conn.update_status(job["id"], JobStatus.FINISHED, progress=100)

    # the JST has been updated in a proper way
    finished_job = _conn.get_object(f"{job['id']}.JST")
    assert finished_job is not None
    assert "status" in finished_job
    assert finished_job["status"] == "FINISHED"
    assert "progress" in finished_job
    assert finished_job["progress"] == "100%"
    # and message and results have not been altered
    assert "messages" in finished_job
    assert finished_job["messages"] == updated_job["messages"]
    assert "calculationResults" in finished_job
    assert calc_results == _calculation_results_as_dict(finished_job["calculationResults"])


@pytest.mark.extended
def test_create_table_should_work_with_ten_million_lines_df(_conn: Connection):
    # when pushing a data source
    data = {
        "column1": [f"key{idx}" for idx in range(10000000)],
        "column2": [idx for idx in range(10000000)],
    }
    dataframe = pd.DataFrame(data)
    data_source_name = "sample_data_source"
    data_source_label = "sample_data_source_label"
    _conn.create_table(
        data_source_name,
        [
            {"name": "column1", "type": "TEXT", "key": True},
            {"name": "column2", "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(dataframe),
        data_source_label,
        replace_existing=True,
    )

    # it can be listed as DMDS via list_objects
    data_sources = _conn.list_objects("DMDS")
    assert len(data_sources) == 1
    assert data_sources[0]["label"] == data_source_label
    assert data_sources[0]["uniqueName"] == data_source_name

    # and it can be pulled with stream_datasource with same content as initial pushed one
    downloaded_content = None
    nb_retry = 0
    while downloaded_content is None and nb_retry < 3:
        try:
            downloaded_content = _csv_stream_to_dataframe(
                _conn.stream_fcs(data_sources[0]["typedId"])
            )
        except HTTPError:
            time.sleep(5)
            nb_retry += 1

    assert downloaded_content is not None, "Failed to download pushed datasource"
    downloaded_content.sort_values(
        ["column1"], axis=0, ignore_index=True, ascending=True, inplace=True
    )
    dataframe.sort_values(["column1"], axis=0, ignore_index=True, ascending=True, inplace=True)
    assert (downloaded_content[list(data.keys())] == dataframe[list(data.keys())]).all().all()
