from collections.abc import AsyncIterator, Iterator
import inspect
from io import BytesIO, StringIO
import json
from typing import Any
import zipfile

import pandas as pd
import pytest

from pyfxlib._testtooling.conftest import (
    _async_conn,
    _auth,
    _conn,
    _job_jst,
    _model_object,
    _pfx_base_url,
    _remote,
    _session,
)
from pyfxlib._testtooling.helpers import (
    _calculation_results_as_dict,
    _IntegrationRemote,
)
from pyfxlib.lowlevel.avro import AvroStream
from pyfxlib.lowlevel.connection import ConnectionAsync, ConnectionSync
from pyfxlib.schema.core import (
    JobStatus,
    Notification,
    NotificationActionType,
    NotificationStatus,
    NotificationTopic,
)
from pyfxlib.schema.lpg import LPG, LPGProduct
from pyfxlib.schema.query import FieldFilter, FilterOperator, Operator

__all__ = [
    "_async_conn",
    "_auth",
    "_conn",
    "_job_jst",
    "_model_object",
    "_pfx_base_url",
    "_remote",
    "_session",
]


def assert_are_equal_for_common_dict_keys(dict_1: dict[str, Any], dict_2: dict[str, Any]) -> None:
    common_keys = set(dict_1.keys()).intersection(dict_2.keys())

    def common_key_dict(dic: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in dic.items() if key in common_keys}

    assert len(common_keys) > 0, "at least one common key required, none found"
    assert common_key_dict(dict_1) == common_key_dict(dict_2)


async def collect_bytes(async_iterator: AsyncIterator[bytes]) -> bytes:
    chunks = []
    async for chunk in async_iterator:
        chunks.append(chunk)
    return b"".join(chunks)


async def collect_csv(async_iterator: AsyncIterator[bytes]) -> pd.DataFrame:
    return pd.read_csv(BytesIO(await collect_bytes(async_iterator)))


@pytest.mark.asyncio
async def test_connection_should_be_able_to_add_an_object_update_it_list_it_and_fetch_it(
    _async_conn: ConnectionAsync,
):
    # when adding a product
    original_label = "aProductlabel"
    updated_label = "anotherProductlabel"
    res = await _async_conn.add_object("P", {"label": original_label, "sku": "the_sku"})

    # then the returned object contains at least the typedId and the given attributes
    assert "typedId" in res.keys()
    assert res["label"] == original_label

    # when updating this object with a new uniqueName
    res["label"] = updated_label
    updated_res = await _async_conn.update_object("P", res)

    # then the returned object is containing the updated version and attributes values
    assert updated_res["label"] == updated_label
    assert updated_res["version"] == res["version"] + 1

    # when getting the list of products
    products = await _async_conn.list_objects("P")

    # then only one exists and updated
    assert len(products) == 1
    assert_are_equal_for_common_dict_keys(products[0], updated_res)

    # and it can be filtered by exact label
    filtered = await _async_conn.list_objects(
        "P",
        filters=[
            FieldFilter(field_name="label", operator=FilterOperator.IEQUALS, value=updated_label)
        ],
    )
    assert len(filtered) == 1
    assert filtered[0]["label"] == updated_label

    # and a non-matching filter returns nothing
    no_match = await _async_conn.list_objects(
        "P",
        filters=[
            FieldFilter(field_name="label", operator=FilterOperator.IEQUALS, value="does_not_exist")
        ],
        filter_aggregator=Operator.AND,
    )
    assert len(no_match) == 0

    # and it can be fetched with get_object
    fetch_pl = await _async_conn.get_object(res["typedId"])
    assert_are_equal_for_common_dict_keys(fetch_pl, updated_res)


@pytest.mark.asyncio
async def test_push_pull_and_list_data_source_should_work_as_expected(_async_conn: ConnectionAsync):
    # when pushing a data source
    data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    dataframe = pd.DataFrame(data)

    data_source_name = "sample_data_source"
    data_source_label = "sample_data_source_label"
    await _async_conn.create_table(
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
    fcs = await _async_conn.list_fcs("DMDS")
    assert len(fcs) == 1
    assert fcs[0]["label"] == data_source_label
    assert fcs[0]["uniqueName"] == data_source_name

    # and it can be fetched using its typedId via get_fc
    data_souce = await _async_conn.get_fc(fcs[0]["typedId"])
    assert data_souce["label"] == data_source_label
    assert data_souce["uniqueName"] == data_source_name

    # and it can be listed as DMDS via list_objects
    data_souces = await _async_conn.list_objects("DMDS")
    assert len(data_souces) == 1
    assert data_souces[0]["label"] == data_source_label
    assert data_souces[0]["uniqueName"] == data_source_name

    # and it can be pulled with stream_datasource with same content as initial pushed one
    downloaded_content = await collect_csv(_async_conn.stream_dm_data(data_souces[0]["typedId"]))
    assert (dataframe[list(data.keys())] == downloaded_content[list(data.keys())]).all().all()


@pytest.mark.asyncio
async def test_should_be_able_to_update_a_data_source(_async_conn: ConnectionAsync):
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
    await _async_conn.create_table(
        data_source_name,
        [
            {"name": "column1", "type": "TEXT", "key": True},
            {"name": "column2", "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(dataframe_init),
        data_source_label,
        replace_existing=True,
    )
    fcs = await _async_conn.list_fcs("DMDS")
    ds_typedid = fcs[0]["typedId"]

    # when update new data
    new_data = {
        "column1": ["key3", "key4"],
        "column2": [2, 24],
    }
    dataframe_append = pd.DataFrame(new_data, index=[3, 4])
    await _async_conn.update_table(ds_typedid, AvroStream.from_dataframe(dataframe_append))

    # then the new content is added to the table
    expected_data = {
        "column1": ["key1", "key2", "key3", "key4"],
        "column2": [1, 12, 2, 24],
    }
    dataframe_expected = pd.DataFrame(expected_data)
    dataframe_downloaded = await collect_csv(_async_conn.stream_dm_data(ds_typedid, 128))
    assert (dataframe_expected == dataframe_downloaded[list(dataframe_expected.keys())]).all().all()


@pytest.mark.asyncio
async def test_should_fail_to_update_a_data_source_when_duplicates(_async_conn: ConnectionAsync):
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
    await _async_conn.create_table(
        data_source_name,
        [
            {"name": "column1", "type": "TEXT", "key": True},
            {"name": "column2", "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(dataframe_init),
        data_source_label,
        replace_existing=True,
    )
    fcs = await _async_conn.list_fcs("DMDS")
    ds_typedid = fcs[0]["typedId"]

    # when update new data
    new_data = {
        "column1": ["key3", "key4", "key3"],
        "column2": [2, 24, 3],
    }
    dataframe_append = pd.DataFrame(new_data, index=[1, 2, 3])

    with pytest.raises(Exception, match="Error while uploading file:"):
        await _async_conn.update_table(ds_typedid, AvroStream.from_dataframe(dataframe_append))

    # then no new content is added to the table
    expected_data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    dataframe_expected = pd.DataFrame(expected_data)
    dataframe_downloaded = await collect_csv(_async_conn.stream_dm_data(ds_typedid, 128))
    assert (dataframe_expected == dataframe_downloaded[list(dataframe_expected.keys())]).all().all()


@pytest.mark.asyncio
async def test_should_be_able_to_update_the_existing_rows_on_a_data_source(
    _async_conn: ConnectionAsync,
):
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
    await _async_conn.create_table(
        data_source_name,
        [
            {"name": "column1", "type": "TEXT", "key": True},
            {"name": "column2", "type": "INTEGER"},
        ],
        AvroStream.from_dataframe(dataframe_init),
        data_source_label,
        replace_existing=True,
    )
    fcs = await _async_conn.list_fcs("DMDS")
    ds_typedid = fcs[0]["typedId"]

    # when update new data
    new_data = {
        "column1": ["key1", "key2"],
        "column2": [2, 24],
    }
    dataframe_append = pd.DataFrame(new_data, index=[1, 2])
    await _async_conn.update_table(ds_typedid, AvroStream.from_dataframe(dataframe_append))

    # then the new content is added to the table
    expected_data = {
        "column1": ["key1", "key2"],
        "column2": [2, 24],
    }
    dataframe_expected = pd.DataFrame(expected_data)
    dataframe_downloaded = await collect_csv(_async_conn.stream_dm_data(ds_typedid, 128))
    assert (dataframe_expected == dataframe_downloaded[list(dataframe_expected.keys())]).all().all()


@pytest.mark.asyncio
async def test_should_be_able_to_attach_a_file_list_attachments_and_pull_an_attachment(
    _async_conn: ConnectionAsync, _model_object: dict[str, Any]
):
    # given an empty model object and an attachment content
    mo_typedid = _model_object["typedId"]
    attachment_name = "anAttachmentName"
    attachment_content = "a first line\n" "a second line\n" "the last line"

    # when attaching a file to the model object
    await _async_conn.attach_file(mo_typedid, attachment_name, StringIO(attachment_content))

    # then this file should be present in the list of its attachments
    attachments = await _async_conn.list_attachments(mo_typedid)
    assert len(attachments) == 1
    assert attachments[0]["fileName"] == attachment_name

    # and it is possible to fetch back its content
    content = await collect_bytes(_async_conn.pull_file(mo_typedid, attachments[0]["typedId"], 128))
    assert attachment_content == content.decode("utf-8")


@pytest.mark.asyncio
async def test_should_be_able_to_push_an_owned_table_and_read_it_back(
    _async_conn: ConnectionAsync, _model_object: dict[str, Any]
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
    await _async_conn.create_table(
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
    fcs = await _async_conn.list_fcs("DMT", {"owner": mo_typedid})
    assert len(fcs) == 1
    assert fcs[0]["name"] == table_name
    assert fcs[0]["label"] == table_label
    assert [x["name"] for x in fcs[0]["fields"]] == list(data.keys())

    # and we can fetch its content
    downloaded_content = await collect_csv(_async_conn.stream_dm_data(fcs[0]["typedId"], 128))
    assert (dataframe[list(data.keys())] == downloaded_content[list(data.keys())]).all().all()


@pytest.mark.asyncio
async def test_should_be_able_to_update_model_table(
    _async_conn: ConnectionAsync, _model_object: dict[str, Any]
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
    await _async_conn.create_table(
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
    fcs = await _async_conn.list_fcs("DMT", {"owner": mo_typedid})
    table_typedid = fcs[0]["typedId"]

    # when appending new data
    index = [2, 3]
    new_data = {
        "column1": ["key2", "key3"],
        "column2": [42, 24],
    }
    dataframe_append = pd.DataFrame(new_data, index=index)
    await _async_conn.update_table(table_typedid, AvroStream.from_dataframe(dataframe_append))

    # then the new content is added to the table
    expected_data = {
        "column1": ["key1", "key2", "key3"],
        "column2": [1, 42, 24],
    }
    dataframe_expected = pd.DataFrame(expected_data)
    dataframe_downloaded = await collect_csv(_async_conn.stream_dm_data(table_typedid, 128))
    assert (dataframe_expected == dataframe_downloaded[list(dataframe_expected.keys())]).all().all()


@pytest.mark.asyncio
async def test_should_fail_to_update_model_table_when_duplicates(
    _async_conn: ConnectionAsync, _model_object: dict[str, Any]
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
    await _async_conn.create_table(
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
    fcs = await _async_conn.list_fcs("DMT", {"owner": mo_typedid})
    table_typedid = fcs[0]["typedId"]

    # when appending new data
    index = [1, 2, 3]
    new_data = {
        "column1": ["key3", "key4", "key3"],
        "column2": [42, 24, 16],
    }
    dataframe_append = pd.DataFrame(new_data, index=index)

    with pytest.raises(Exception, match="Error while uploading file:"):
        await _async_conn.update_table(table_typedid, AvroStream.from_dataframe(dataframe_append))

    # then the no new content is added to the table
    expected_data = {
        "column1": ["key1", "key2"],
        "column2": [1, 12],
    }
    dataframe_expected = pd.DataFrame(expected_data)
    dataframe_downloaded = await collect_csv(_async_conn.stream_dm_data(table_typedid, 128))
    assert (dataframe_expected == dataframe_downloaded[list(dataframe_expected.keys())]).all().all()


@pytest.mark.asyncio
async def test_should_be_able_update_the_existing_rows_on_a_model_table(
    _async_conn: ConnectionAsync, _model_object: dict[str, Any]
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
    await _async_conn.create_table(
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
    fcs = await _async_conn.list_fcs("DMT", {"owner": mo_typedid})
    table_typedid = fcs[0]["typedId"]

    # when appending new data
    index = [1, 2]
    new_data = {
        "column1": ["key1", "key2"],
        "column2": [2, 24],
    }
    dataframe_append = pd.DataFrame(new_data, index=index)
    await _async_conn.update_table(table_typedid, AvroStream.from_dataframe(dataframe_append))

    # then the new content is added to the table
    expected_data = {
        "column1": ["key1", "key2"],
        "column2": [2, 24],
    }
    dataframe_expected = pd.DataFrame(expected_data)
    dataframe_downloaded = await collect_csv(_async_conn.stream_dm_data(table_typedid, 128))
    assert (dataframe_expected == dataframe_downloaded[list(dataframe_expected.keys())]).all().all()


@pytest.mark.asyncio
async def test_should_be_able_to_update_a_job_status(
    _remote: _IntegrationRemote,
    _async_conn: ConnectionAsync,
    _model_object: dict[str, Any],
    _job_jst: dict[str, Any],
):
    job_id = _job_jst["id"]
    calc_results = {"foo": 12, "bar": "baz"}
    message = "almost finished"
    # when updating the status of the job
    await _async_conn.update_status(
        job_id,
        JobStatus.PROCESSING,
        42,
        message,
        calc_results,
    )

    # the JST has been updated in a proper way
    updated_job = await _async_conn.get_object(f"{job_id}.JST")
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
    await _async_conn.update_status(job_id, JobStatus.FINISHED, progress=100)

    finished_job = await _async_conn.get_object(f"{job_id}.JST")
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


@pytest.mark.asyncio
@pytest.mark.extended
async def test_create_table_should_work_with_ten_million_lines_df(_async_conn: ConnectionAsync):
    # when pushing a data source
    data = {
        "column1": [f"key{idx}" for idx in range(10000000)],
        "column2": [idx for idx in range(10000000)],
    }
    dataframe = pd.DataFrame(data)
    data_source_name = "sample_data_source"
    data_source_label = "sample_data_source_label"
    await _async_conn.create_table(
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
    data_sources = await _async_conn.list_objects("DMDS")
    assert len(data_sources) == 1
    assert data_sources[0]["label"] == data_source_label
    assert data_sources[0]["uniqueName"] == data_source_name

    # and it can be pulled with stream_datasource with same content as initial pushed one
    dataframes = []
    async for page in _async_conn.fetch_paginated_dm_data(data_sources[0]["typedId"]):
        dataframes.append(pd.DataFrame(page))

    downloaded_content = pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()
    assert downloaded_content is not None, "Failed to download pushed datasource"
    assert len(downloaded_content) == len(dataframe)

    downloaded_content.sort_values(
        ["column1"], axis=0, ignore_index=True, ascending=True, inplace=True
    )
    dataframe.sort_values(["column1"], axis=0, ignore_index=True, ascending=True, inplace=True)
    assert (downloaded_content[list(data.keys())] == dataframe[list(data.keys())]).all().all()


def test_connection_sync_returns_values_not_coroutines(_conn: ConnectionSync):
    # given a data source
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

    # when calling a method of the connection using _run_sync
    list_dmds = _conn.list_fcs("DMDS")
    assert not inspect.iscoroutine(list_dmds)
    assert isinstance(list_dmds, list)
    assert "typedId" in list_dmds[0]

    # when using a method of the connection using _sync_iterator
    chunks = _conn.stream_dm_data(list_dmds[0]["typedId"])
    assert not inspect.iscoroutine(chunks)
    assert isinstance(chunks, Iterator)


@pytest.mark.asyncio
async def test_backend_version_should_return_version_dict(_async_conn: ConnectionAsync):
    version = await _async_conn.backend_version()
    print(f"Backend version: {version}")
    assert isinstance(version["major"], int)
    assert version["minor"] is None or isinstance(version["minor"], int)
    assert version["patch"] is None or isinstance(version["patch"], int)


@pytest.mark.asyncio
async def test_login_extended_should_return_backend_release(_async_conn: ConnectionAsync):
    result = await _async_conn.login_extended()
    assert result["response"]["data"][0]["extendedData"]["Release"] is not None


@pytest.mark.asyncio
async def test_send_notification_should_succeed(_async_conn: ConnectionAsync):
    version = await _async_conn.backend_version()
    if (version["major"] or 0) < 15 or (
        (version["major"] or 0) == 15 and (version["minor"] or 0) < 2
    ):
        pytest.skip("Notifications not supported by backend version < 15.2")
    notif = Notification(
        title="My Title",
        message="Description of banner",
        source="notificationBanner",
        status=NotificationStatus.INFO,
        topic=NotificationTopic.SYSTEM_NOTIFICATION,
        action_type=NotificationActionType.INFO_MESSAGE,
        valid_from="2025-06-02T23:00:00.016Z",
        valid_until="2025-06-03T21:59:59.016Z",
        due_date="2025-06-03T21:59:59.016Z",
        dismissible=True,
    )
    result = await _async_conn.send_notification(notif)
    assert "data" in result


@pytest.mark.asyncio
async def test_list_users_should_return_valid_users(_async_conn: ConnectionAsync):
    # given a user with email and a user without
    await _async_conn.add_object(
        "U",
        {
            "loginName": "test.with.email",
            "email": "test.with.email@pricefx.com",
        },
    )
    await _async_conn.add_object(
        "U",
        {
            "loginName": "test.without.email",
        },
    )
    # when listing users
    result = await _async_conn.list_users()
    # then only the user with email is returned
    login_names = [u.login_name for u in result]
    assert "test.with.email" in login_names
    assert "test.without.email" not in login_names
    # and the returned user has the expected structure
    user = next(u for u in result if u.login_name == "test.with.email")
    assert user.email == "test.with.email@pricefx.com"


@pytest.mark.asyncio
async def test_get_application_property_should_return_values_as_list(_async_conn: ConnectionAsync):
    result = await _async_conn.get_application_property("clusterName")

    assert isinstance(result, list)
    assert len(result) > 0
    assert all(isinstance(v, str) for v in result)


@pytest.mark.asyncio
async def test_lpg_minimal_operations(_async_conn: ConnectionAsync):
    lpg = await _async_conn.add_object(
        "PG",
        {
            "priceGridType": "SIMPLE",
            "label": "Test LPG",
        },
    )
    lpg_model = LPG.model_validate(lpg)
    lpg_id = lpg_model.id
    assert lpg_model.type == "SIMPLE"

    items = await _async_conn.list_lpg_items(lpg_id)
    assert isinstance(items, list)
    assert all(isinstance(item, LPGProduct) for item in items)

    metadata = await _async_conn.get_object_metadata("PGIM", "priceGridId", lpg_id)
    assert isinstance(metadata, list)


@pytest.mark.asyncio
async def test_push_and_get_calcitems(_async_conn: ConnectionAsync):
    # given a lookup table
    lt = await _async_conn.add_object(
        "LT",
        {
            "uniqueName": "test_lookup_table",
            "label": "Test LT",
            "type": "JSON",
            "valueType": "JSON2",
        },
    )
    lt_typedid = lt["typedId"]
    # when pushing two calcitems
    await _async_conn.push_calcitem(lt_typedid, "K1", "K2", 42)
    await _async_conn.push_calcitem(lt_typedid, "K1", "K2bis", 73)
    # then they are retrievable
    items = await _async_conn.get_calcitems(lt_typedid)
    assert isinstance(items, list)
    assert len(items) == 2
    expected = [
        {"key1": "K1", "key2": "K2", "attributeExtension___Value": "42"},
        {"key1": "K1", "key2": "K2bis", "attributeExtension___Value": "73"},
    ]
    for exp in expected:
        assert any(
            all(item.get(key) == expected_value for key, expected_value in exp.items())
            for item in items
        )


@pytest.mark.asyncio
async def test_import_files(_async_conn: ConnectionAsync, _model_object: dict[str, Any]):
    # given a model object zipped as mo.json
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("mo.json", json.dumps(_model_object, indent=2))
    files = {"file": ("mo.zip", zip_buffer.getvalue(), "application/zip")}
    # when importing it
    typed_id = await _async_conn.import_files(files)
    # then we get back a valid MO typedId
    assert isinstance(typed_id, str)
    assert typed_id.endswith(".MO")


@pytest.mark.asyncio
async def test_query_meta_and_execute(_async_conn: ConnectionAsync):
    # given a product
    sku = "query_test_sku"
    label = "query_test_label"
    await _async_conn.add_object("P", {"sku": sku, "label": label})
    query = {
        "kind": "pipeline",
        "stages": [
            {
                "kind": "source",
                "table": {"kind": "products"},
            },
            {"kind": "take", "count": 5},
        ],
    }
    # when fetching metadata
    meta = await _async_conn.query_meta(query)
    # then the metadata describes the expected columns
    column_names = [col.name for col in meta.columns]
    assert "sku" in column_names
    assert "label" in column_names
    # when executing the query
    rows = await _async_conn.query_execute(query)
    # then the result contains the created product
    assert isinstance(rows, list)
    assert any(
        row[column_names.index("sku")] == sku and row[column_names.index("label")] == label
        for row in rows
    )


@pytest.mark.asyncio
async def test_create_action(_async_conn: ConnectionAsync):
    # given a user to assign the action to
    user = await _async_conn.add_object(
        "U",
        {
            "loginName": "action_assignee",
            "email": "action_assignee@pricefx.com",
        },
    )
    assignee_id = int(user["typedId"].split(".")[0])
    # when creating an action
    action = await _async_conn.create_action(
        title="Test Action",
        assignee_id=assignee_id,
        due_date="2030-12-31",
        description="A test action description",
    )
    # then the result is a valid open action item assigned to the right user
    assert action["typedId"].endswith(".AI")
    assert action["summary"] == "Test Action"
    assert action["assignedTo"] == assignee_id
    assert action["status"] == "OPEN"
    assert action["description"] == "A test action description"
