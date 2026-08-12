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

"""Interacting with a Pricefx platform directly.

This platform defines a low-level wrapper to interact with the Pricefx platform.
For a higher level API, see the `pyfxlib.api.domain` package.

"""

from abc import ABC, abstractmethod
import asyncio
from collections.abc import AsyncIterator, Coroutine, Iterator, Sequence
import csv
from io import BytesIO, TextIOBase
import json
import logging
import mimetypes
import os
from pathlib import Path
import re
import threading
from typing import Any, cast, IO, Optional, TypeVar

from httpx import HTTPStatusError
import pandas as pd

from pyfxlib.lowlevel.avro import AvroStream
from pyfxlib.lowlevel.constants import _DEFAULT_PAGE_SIZE, _DEFAULT_STREAM_CHUNK_SIZE
from pyfxlib.lowlevel.session import PfxSession
from pyfxlib.schema import (
    AdvancedCriteria,
    FieldRule,
    FilterOperator,
    JobStatus,
    LPGProduct,
    Notification,
    Operator,
    QueryAnswerMeta,
    UserInfo,
)

T = TypeVar("T")


LOGGER = logging.getLogger(__name__)


class ConnectionAsync(ABC):
    """Abstract class for an async Connection.

    A connection abstract the low-lever details of interacting with a "backend".
    This backend can be an actual Pricefx instance, or can be substituted with,
    e.g. a local mock.
    """

    @staticmethod
    def _split_typedid(typed_id: str) -> tuple[int, str]:
        match = re.search(r"^(?P<id>[0-9]+)\.(?P<type_code>[A-Z]+)$", typed_id)
        if match:
            return (int(match.group("id")), match.group("type_code"))
        raise ValueError(f"'{typed_id}' is not a valid typedId")

    @abstractmethod
    async def get(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Make a GET request to the backend.

        To use only when the endpoint access is not implemented in Pyfxlib.
        """
        pass

    @abstractmethod
    async def post(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Make a POST request to the backend.

        To use only when the endpoint access is not implemented in Pyfxlib.
        """
        pass

    @abstractmethod
    async def login_extended(self) -> dict[str, Any]:
        """Calls POST login/extended and returns the raw JSON response.

        Usually used to validate the authentication token (raises an error if the token is invalid)
        or for the backen_version method to get the backend version from the response.
        """
        pass

    @abstractmethod
    async def backend_version(self) -> dict[str, Optional[int]]:
        """Fetch the backend version information.

        Returns:
            A dict containing at least the major version,
            optionally the minor and the patch values,
             e.g. {"major": 15, "minor": 2, "patch": 0}.
        """
        pass

    @abstractmethod
    async def send_notification(self, notification: Notification) -> dict[str, Any]:
        """Send a notification to the user via the backend.

        See https://pricefx.atlassian.net/wiki/spaces/UNITY/pages/5482283105/App+Notifications
        and https://api.pricefx.com/openapi/reference/pricefx-server_openapi/notifications/post-notification.send # noqa: E501
        for details on allowed values in the request body.

        Returns the backend response as a dictionary.
        Notifications are not supported by the BE versions < 15.2 (raises an error)
        """
        pass

    @abstractmethod
    async def list_users(self, criteria: Optional[AdvancedCriteria] = None) -> list[UserInfo]:
        """List all users available in the system.

        Args:
            criteria: optional advanced filters to apply when listing users.
                      If not provided, all users will be returned.
        """
        pass

    @abstractmethod
    async def get_application_property(self, property_name: str) -> list[str]:
        """Fetches the values of an application property by name.

        Args:
            property_name: The name of the property.
        Returns:
            The list of values associated with the property.
        """
        pass

    @abstractmethod
    async def get_object(
        self,
        typedid: str,
    ) -> dict[str, Any]:
        """Get the attributes of a specific element.

        Args:
            typedid: the typedId of the element, in the format "{id}.{typeCode}"
        Returns:
            The corresponding object with all fields,
            as defined in the Pricefx REST API public documentation.
        Raises:
            ValueError: if no object is found for the given typedId
        """
        pass

    @abstractmethod
    async def list_objects(
        self,
        type_code: str,
        filters: Optional[dict[str, Any] | Sequence[FieldRule] | AdvancedCriteria] = None,
        filter_aggregator: Optional[Operator] = None,
        start_row: Optional[int] = None,
        max_rows: Optional[int] = None,
        sort_by: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """List all the elements of a given type with optional filtering and pagination.

        Common types: P (Products), C (Customers), PL (Price Lists), Q (Quotes),
                      MO (Model Objects), DMM (DM Models), DMT (DM Tables),
                      DMDS (DM Data Sources)...
        If you need the full list of TypeCodes, search the knowledge base for "Type Codes".

        Args:
            type_code: Entity TypeCode
            filters: a `{field: value}` dict, a sequence of `FieldRule`, or an
                `AdvancedCriteria` (in which case `filter_aggregator` is ignored)
            filter_aggregator: logical operator to combine multiple filters (default: Operator.AND)
            start_row: Starting index (0-based)
            max_rows: Max results (1-200, default 5)
            sort: field on which to sort the data

        Returns:
            List of entity objects with all fields,
            as defined in the Pricefx REST API public documentation.
        """
        pass

    @abstractmethod
    async def get_object_metadata(
        self,
        type_code: str,
        payload_key: str,
        object_id: int,
    ) -> list[dict[str, Any]]:
        """Get the metadata of a specific element.

        Common type: PGIM (Price Grid Item Attribute Meta)
        If you need the full list of TypeCodes, search the knowledge base for "Type Codes".

        Args:
            type_code: Entity TypeCode
            payload_key: the key corresponding to the type_code to put in the body of the request
            object_id: Id of the object
        """
        pass

    @abstractmethod
    async def add_object(
        self,
        type_code: str,
        attributes: dict[str, Any],
    ) -> dict[str, Any]:
        """Add an object and returns its actual attributes."""
        pass

    @abstractmethod
    async def update_object(
        self,
        type_code: str,
        attributes: dict[str, Any],
    ) -> dict[str, Any]:
        """Update an object and returns its actual attributes."""
        pass

    @abstractmethod
    async def quick_search(self, sku_or_label: str) -> list[dict[str, Any]]:
        """Search for all lists containing a product designated by its sku or label.

        Note that the search is case-sensitive.
        """
        pass

    @abstractmethod
    async def get_fc(
        self,
        typedid: str,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Get the attributes of a specific field collection."""
        pass

    @abstractmethod
    async def list_fcs(
        self,
        type_code: str,
        params: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """List all the fields collections of a given type."""
        pass

    @abstractmethod
    async def create_table(
        self,
        name: str,
        fields_spec: list[dict],
        content: AvroStream,
        label: Optional[str] = None,
        owner_typedid: Optional[str] = None,
        replace_existing: bool = True,
    ) -> None:
        """Create a table in the backend.

        Args:
            name: the name of the table to push
            fields_spec: a list of dictionaries that describe the columns of the data source
            ```json
            [
               { name:"productId", label:"Product ID", type:"TEXT", key:true},
               { name:"productGroup", label:"ProductGroup", type:"TEXT", dimension:true},
               { name:"revenue", label:"Revenue", type:"MONEY"},
            ]
            ```
            content: the content of the source file to push in avro format
            label: the label of the table (optional, defaults to name)
            owner_typedid: the name of the table owner (optional, set it to create a DMT)
            replace_existing: if True then removes the preceding data source having the same name if
                              it exists before pushing the data (default = True).
        """
        pass

    @abstractmethod
    async def update_table(self, typedid: str, data: AvroStream) -> None:
        """Update data of an already existing DMTable or DMDataSource.

        Values of rows with same keys will be updated.
        Rows with new keys will be appended.

        Args:
            typedid: the typed id of the table to append
            data: the content of the data to push in avro format
        """
        pass

    @abstractmethod
    async def stream_dm_data(
        self,
        typedid: str,
        chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE,
        criteria: Optional[AdvancedCriteria] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> AsyncIterator[bytes]:
        """Stream the content of a data source.

        Args:
            typedid: the typed id of the data source to stream
            chunk_size: the size in bytes of the yielded chunks
            criteria: optional server-side row filter
            columns: optional list of columns to keep (default: all)
        """
        yield b""

    @abstractmethod
    async def fetch_paginated_dm_data(
        self,
        typedid: str,
        page_size: int = _DEFAULT_PAGE_SIZE,
        criteria: Optional[AdvancedCriteria] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Fetch the content of a data source using paginated requests.

        More reliable than stream_dm_data for large datasets.

        Args:
            typedid: the typed id of the data source to fetch
            page_size: the number of rows to fetch per page
            criteria: optional server-side row filter
            columns: optional list of columns to keep (default: all)
        """
        yield []

    @abstractmethod
    async def get_calcitems(self, typedid: str) -> list[dict[str, Any]]:
        """Get all rows from a JSON2-type Lookup Table.

        Note: only supports lookup tables of type JSON / valueType JSON2.

        Args:
            typedid: typedId of the table (e.g. "2623.JLTV2"), only the id number is used.
        Returns:
            List of all rows. Each row contains key1, key2, typedId of the parent table,
            and the value stored under attributeExtension___Value.
        """
        pass

    @abstractmethod
    async def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """Add a row to a JSON2-type Lookup Table.

        Note: only supports LTs of type JSON / valueType JSON2.
        The value is stored under the attributeExtension___Value field.

        Args:
            typedid: typedId of the lookup table (e.g. "2623.JLTV2"), only the id number is used.
            key1: first key
            key2: second key
            value: value to store
        """
        pass

    @abstractmethod
    async def list_lpg_items(
        self, lpg_id: int, filters: Optional[dict[str, Any]] = None
    ) -> list[LPGProduct]:
        """Fetch the item data from an LPG.

        Args:
            lpg_id: Id of the LPG
            filters: a dict of filters in the format {fieldName: value, ...} (optional)
                They are supposed to use an iequals operator, and will be aggregated with an "and".
        Returns:
            List of LPG products with all fields,
            as defined in the Pricefx REST API public documentation.
        """
        pass

    @abstractmethod
    async def update_lpg(
        self,
        lpg_id: int,
        field_to_update: str,
        new_value: float,
        product: dict[str, Any],
        comment: str,
    ) -> None:
        """Update the value of a field of a product in an LPG.

        Raise ValueError if an LPG of type MATRIX is passed without a secondary key.
        """
        pass

    @abstractmethod
    async def submit_lpg(self, lpg_id: int, product_typedids: list[str]) -> list[dict[str, Any]]:
        """Accept changes to LPG items in the workflow.

        Unsubmitted items will be submitted.
        Already submitted items will be approved.

        Note: the endpoint only returns data when
        a single id is passed, otherwise "data" is null

        Args:
            lpg_id: Id of the LPG
            product_typedids: list of typedIds of the products to submit,
                              in the format "{id}.{typeCode}"
        Returns:
            A dict representing a product, as defined in the Pricefx REST API public documentation
            if a single typedId is passed, otherwise an empty list.
        """
        pass

    @abstractmethod
    async def list_attachments(
        self,
        typedid: str,
    ) -> list[dict[str, Any]]:
        """Get the information about attachments of the model."""
        pass

    @abstractmethod
    async def attach_file(
        self,
        typedid: str,
        name: str,
        content: IO,
    ) -> None:
        """Attach a content as a file with a given name to the model."""
        pass

    @abstractmethod
    async def pull_file(
        self,
        owner_typedid: str,
        attachment_typedid: str,
        chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE,
    ) -> AsyncIterator[bytes]:
        """Fetch file from the backend."""
        yield b""

    @abstractmethod
    async def import_files(self, files: dict[str, tuple[Optional[str], bytes | str, str]]) -> str:
        """Import files to the backend.

        Args:
            files:
                - for core < 15.0.0:
                    {"data": (None, json dump of file metadata, "application/json")}
                    with file metadata in the format {"uniqueName": ..., "label": ...}
                - for core >= 15.0.0:
                    {"file": (filename, file content as bytes, "application/zip")}
        Returns:
            The typed id of the imported object
        """
        pass

    @abstractmethod
    async def update_status(
        self,
        jst_id: int,
        status_code: JobStatus,
        progress: Optional[int],
        msg: Optional[str] = None,
        results: Optional[dict[str, Any]] = None,
    ) -> None:
        """Update the job status on the backend.

        Args:
            jst_id: the id of the jst
            status_code: the new job status
            progress: the new job progress percent (optional)
            msg: a message for the new status change (optional)
            results: a dictionary associating result names to their value (optional).
                     Will be available to next evaluations and calculations.
        """
        pass

    @abstractmethod
    async def query_meta(self, query: dict[str, Any]) -> QueryAnswerMeta:
        """Fetches the schema of a queryAPI query result: column names and types.

        See: https://pricefx.atlassian.net/wiki/spaces/KB/pages/5753667585/Pipeline+Queries+QueryAPI

        Args:
            query: The queryAPI query definition.
        Returns:
            The query result metadata (column names and types) as returned by the backend.
        """
        pass

    @abstractmethod
    async def query_execute(self, query: dict[str, Any]) -> list[list[Any]]:
        """Executes a queryAPI and returns the result rows.

        See: https://pricefx.atlassian.net/wiki/spaces/KB/pages/5753667585/Pipeline+Queries+QueryAPI

        Args:
            query: The queryAPI query definition.
        Returns:
            The query results as a list of rows, each row being a list of values.
        """
        pass

    @abstractmethod
    async def create_action(
        self,
        title: str,
        assignee_id: int,
        due_date: str,
        description: str,
        recommendations: Optional[str] = None,
        originator_typed_id: Optional[str] = None,
        dashboard_inputs: Optional[dict[str, Any]] = None,
        dashboard_preferences: Optional[dict[str, Any]] = None,
        action_item_type: str = "__DEFAULT__",
    ) -> dict[str, Any]:
        """Create an action item.

        Args:
            recommendations: in HTML format
        Returns:
            A dictionary representing the created action item as returned by the backend.
        """
        pass


class ConnectionRemote(ConnectionAsync):
    """A connection to a remote instance.

    This class abstracts the various aspects of interacting with the Pricefx platform behind an
    unified interface.
    """

    # By default, stream download timeout is 60s which may cause errors when the table is large.
    # (see https://pricefx.atlassian.net/browse/PFUN-14665)
    # When the back end receives a timeout, it chooses an actual timeout which is the minimum value
    # between a configured "MAX TIMEOUT" value and the timeout argument.
    # The solution here is to set the timeout to 1h so that the back end will always choose this
    # configured "MAX TIMEOUT" value up to 1h.
    _DM_FETCH_TIMEOUT = 3600

    def __init__(self, endpoint: str, pfxsession: PfxSession) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.session = pfxsession

    def __repr__(self) -> str:
        return f"ConnectionRemote({self.endpoint})"

    async def _fc_spec(self, typedid: str) -> Optional[dict]:
        objectid = typedid.split(".")[0]
        fc_type = typedid.split(".")[1]
        response_value = await self.session.post(
            f"{self.endpoint}/datamart.getfcs/{fc_type}",
            json={
                "data": {
                    "id": objectid,
                }
            },
        )
        response = response_value.json()
        if len(response["response"]["data"]) > 0:
            return response["response"]["data"][0]
        return None

    async def get(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.get(f"{self.endpoint}/{path.lstrip('/')}", **kwargs)
        return response.json()

    async def post(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(f"{self.endpoint}/{path.lstrip('/')}", **kwargs)
        return response.json()

    async def login_extended(self) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(f"{self.endpoint}/login/extended")
        return response.json()

    async def backend_version(self) -> dict[str, Optional[int]]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.login_extended()
        backend_version = str(response["response"]["data"][0]["extendedData"]["Release"])
        try:
            if backend_version.endswith("-SNAPSHOT"):
                backend_version = backend_version[: -len("-SNAPSHOT")]
            splitted = [int(v) for v in backend_version.split(".")]
            return {"major": None, "minor": None, "patch": None} | {
                k: v for k, v in zip(["major", "minor", "patch"], splitted)
            }
        except ValueError as err:
            raise ValueError(
                f"Invalid version format: {backend_version}. Expected format is 'major.minor.patch' or 'major.minor' or 'major' with int values.",  # noqa: E501
            ) from err

    async def send_notification(self, notification: Notification) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        backend_version = await self.backend_version()
        if ((backend_version["major"] or 0) < 15) or (
            (backend_version["major"] or 0) == 15 and (backend_version["minor"] or 0) < 2
        ):
            raise RuntimeError(
                "Notifications are not supported by backend versions below 15.2. "
                f"Current backend version: {backend_version}"
            )
        response = await self.session.post(
            f"{self.endpoint}/notification.send",
            json={"data": {"notification": notification.model_dump(exclude_none=True)}},
        )
        return response.json()["response"]

    async def list_users(self, criteria: Optional[AdvancedCriteria] = None) -> list[UserInfo]:
        """See `ConnectionAsync` corresponding method."""
        body: dict[str, Any] = {"operationType": "fetch", "textMatchStyle": "exact"}
        if criteria is not None:
            body["data"] = criteria.model_dump()
        response = await self.session.post(f"{self.endpoint}/accountmanager.fetchusers", json=body)
        return [
            UserInfo.model_validate(user_info)
            for user_info in response.json()["response"]["data"]
            if user_info.get("email") is not None
        ]

    async def get_application_property(self, property_name: str) -> list[str]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.get(
            f"{self.endpoint}/configurationmanager.get/{property_name}"
        )
        return response.json()["response"]["data"]

    async def get_object(
        self,
        typedid: str,
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        id, type_code = self._split_typedid(typedid)
        response = await self.session.post(f"{self.endpoint}/fetch/{type_code}/{id}")
        data = response.json()["response"]["data"]
        if data is not None:
            return data[0]
        else:
            raise ValueError(f"Object with typedId '{typedid}' not found")

    async def list_objects(
        self,
        type_code: str,
        filters: Optional[dict[str, Any] | Sequence[FieldRule] | AdvancedCriteria] = None,
        filter_aggregator: Optional[Operator] = None,
        start_row: Optional[int] = None,
        max_rows: Optional[int] = None,
        sort_by: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        start_row = start_row or 0
        end_row = start_row + max(min(max_rows, 200), 1) if max_rows is not None else None
        criteria = (
            None
            if filters is None
            else AdvancedCriteria.from_filters(filters, filter_aggregator or Operator.AND)
        )
        body = {
            "startRow": start_row,
            "endRow": end_row,
            "operationType": "fetch",
            "textMatchStyle": "exact",
            "data": criteria.model_dump() if criteria else None,
            "sortBy": [sort_by] if sort_by else None,
        }
        response = await self.session.post(f"{self.endpoint}/fetch/{type_code}", json=body)
        return response.json()["response"]["data"]

    async def get_object_metadata(
        self,
        type_code: str,
        payload_key: str,
        object_id: int,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        payload = {"data": {payload_key: str(object_id)}}
        response = await self.session.post(f"{self.endpoint}/fetch/{type_code}", json=payload)
        return response.json()["response"]["data"]

    async def add_object(self, type_code: str, attributes: dict[str, Any]) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(
            f"{self.endpoint}/add/{type_code}",
            json={"data": attributes},
        )
        return response.json()["response"]["data"][0]

    async def update_object(self, type_code: str, attributes: dict[str, Any]) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(
            f"{self.endpoint}/update/{type_code}",
            json={"data": attributes},
        )
        return response.json()["response"]["data"][0]

    async def quick_search(self, sku_or_label: str) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(
            f"{self.endpoint}/productmanager.quicksearch/{sku_or_label}"
        )
        return response.json()["response"]["data"]

    async def get_fc(self, typedid: str, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(
            f"{self.endpoint}/datamart.getfcs/{typedid}",
            json={"data": params},
        )
        return response.json()["response"]["data"][0]

    async def list_fcs(
        self, type_code: str, params: Optional[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(
            f"{self.endpoint}/datamart.getfcs/{type_code}", json={"data": params}
        )
        return response.json()["response"]["data"]

    async def create_table(
        self,
        name: str,
        fields_spec: list[dict],
        content: AvroStream,
        label: Optional[str] = None,
        owner_typedid: Optional[str] = None,
        replace_existing: bool = True,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        typecode = "DMT" if owner_typedid is not None else "DMDS"

        schema: dict[str, Any] = {
            "label": label if label is not None else name,
            "fields": fields_spec,
        }

        if typecode == "DMT":
            schema["name"] = name
            schema["owner"] = owner_typedid
        else:
            schema["uniqueName"] = name

        files = {
            "DMFieldCollectionSpec": (
                None,
                json.dumps(schema),
                "text/json; charset=UTF-8",
            ),
            "DMFieldCollectionData": (
                None,
                content,
                "avro/binary",
            ),
        }
        await self.session.post_simple(
            f'{self.endpoint}/datamart.createfc/{typecode}{"/replace" if replace_existing else ""}',
            files=files,
        )

    async def update_table(self, typedid: str, data: AvroStream) -> None:
        """See `ConnectionAsync` corresponding method."""
        fc_entry = await self._fc_spec(typedid)
        if fc_entry is None:
            raise RuntimeError(f"Trying to append to non-existing table '{typedid}'")
        response_value = await self.session.post(f"{self.endpoint}/uploadmanager.newuploadslot")
        response = response_value.json()
        uploadslot = response["response"]["data"][0]["id"]
        try:
            files = {"DMFieldCollectionData": ("data.avro", data, "avro/binary")}
            await self.session.post_simple(
                f"{self.endpoint}/datamart.loadfc/{typedid}", files=files
            )
        except HTTPStatusError as err:
            error_value = await self.session.post(
                f"{self.endpoint}/uploadmanager.progress/{uploadslot}",
            )
            error_desc = error_value.json()["response"]["data"][0]["data"]
            raise Exception(f"Error while uploading file: {error_desc}") from err
        finally:
            await self.session.post(f"{self.endpoint}/uploadmanager.deleteslot/{uploadslot}")

    async def stream_dm_data(
        self,
        typedid: str,
        chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE,
        criteria: Optional[AdvancedCriteria] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> AsyncIterator[bytes]:
        """See `ConnectionAsync` corresponding method."""
        body: dict[str, Any] = {}
        if criteria is not None:
            body["data"] = criteria.model_dump()
        if columns is not None:
            body["resultFields"] = list(columns)
        async for chunk in self.session.get_stream(
            f"{self.endpoint}/datamart.fetch/{typedid}?stream&timeout={self._DM_FETCH_TIMEOUT}",
            chunk_size=chunk_size,
            params={"output": "csv"},
            json=body or None,
        ):
            yield chunk

    async def fetch_paginated_dm_data(
        self,
        typedid: str,
        page_size: int = _DEFAULT_PAGE_SIZE,
        criteria: Optional[AdvancedCriteria] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """See `ConnectionAsync` corresponding method."""
        sort_by: list[str] = []
        fc_meta = await self.get_fc(typedid)
        sort_by = [field["name"] for field in fc_meta.get("fields", []) if field.get("key", False)]

        start_row = 0
        while True:
            response = await self.session.post(
                f"{self.endpoint}/datamart.fetch/{typedid}",
                json={
                    "startRow": start_row,
                    "endRow": start_row + page_size,
                    "sortBy": sort_by,
                    "data": criteria.model_dump() if criteria else None,
                    "resultFields": list(columns) if columns else None,
                },
            )
            data = response.json()["response"]["data"]
            if not data:
                break
            yield data
            start_row += page_size

    async def get_calcitems(self, typedid: str) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        url = f"{self.endpoint}/lookuptablemanager.fetch/{self._split_typedid(typedid)[0]}"
        response = await self.session.post(url)
        return response.json()["response"]["data"]

    async def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """See `ConnectionAsync` corresponding method."""
        url = f"{self.endpoint}/lookuptablemanager.add/{self._split_typedid(typedid)[0]}"
        await self.session.post(
            url,
            json={
                "data": {
                    "key1": key1,
                    "key2": key2,
                    "attributeExtension___Value": value,
                },
            },
        )

    async def list_lpg_items(
        self, lpg_id: int, filters: Optional[dict[str, Any]] = None
    ) -> list[LPGProduct]:
        """See `ConnectionAsync` corresponding method."""
        criteria = None
        if filters:
            criteria = AdvancedCriteria(
                operator=Operator.AND,
                criteria=[
                    FieldRule(field_name=key, operator=FilterOperator.IEQUALS, value=value)
                    for key, value in filters.items()
                ],
            )
        response = await self.session.post(
            f"{self.endpoint}/pricegridmanager.fetch/{lpg_id}",
            json={"data": criteria.model_dump() if criteria else None},
        )
        return response.json()["response"]["data"]

    async def update_lpg(
        self,
        lpg_id: int,
        field_to_update: str,
        new_value: float,
        product: dict[str, Any],
        comment: str,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        payload = {
            "data": {
                "typedId": product["typedId"],
                field_to_update: new_value,
                "comments": comment,
            },
            "oldValues": product,
            "operationType": "update",
            "textMatchStyle": "exact",
        }
        await self.session.post(f"{self.endpoint}/pricegridmanager.update/{lpg_id}", json=payload)

    async def submit_lpg(self, lpg_id: int, product_typedids: list[str]) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        payload = {"data": {"ids": product_typedids}}
        response = await self.session.post(
            f"{self.endpoint}/pricegridmanager.accept/{lpg_id}", json=payload
        )
        return response.json()["response"]["data"]

    async def list_attachments(
        self,
        typedid: str,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(f"{self.endpoint}/bdmanager.list/{typedid}")
        return response.json()["response"]["data"]

    async def attach_file(
        self,
        typedid: str,
        name: str,
        content: IO,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(f"{self.endpoint}/uploadmanager.newuploadslot")
        slot_id = response.json()["response"]["data"][0]["id"]

        try:
            attachments = await self.session.post(f"{self.endpoint}/bdmanager.list/" + typedid)
            existing = [
                attachment["typedId"].partition(".BD")[0]
                for attachment in attachments.json()["response"]["data"]
                if attachment["fileName"] == name
            ]

            if existing:
                url = f"{self.endpoint}/bdmanager.edit/{typedid}/{existing[0]}/{slot_id}"
            else:
                url = f"{self.endpoint}/bdmanager.upload/{typedid}/{slot_id}"

            if isinstance(content, TextIOBase):
                content = BytesIO(content.read().encode("utf-8"))
            await self.session.post(
                url,
                files={name: (name, content, "text/plain")},
            )
        finally:
            await self.session.post(f"{self.endpoint}/uploadmanager.deleteslot/{slot_id}")

    async def pull_file(
        self,
        owner_typedid: str,
        attachment_typedid: str,
        chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE,
    ) -> AsyncIterator[bytes]:
        """See `ConnectionAsync` corresponding method."""
        attachment_id = attachment_typedid.partition(".BD")[0]
        try:
            async for chunk in self.session.get_stream(
                f"{self.endpoint}/bdmanager.download/{owner_typedid}/{attachment_id}",
                chunk_size=chunk_size,
                params={"output": "file"},
            ):
                yield chunk
        except Exception as e:
            raise Exception(f"Error while fetching attached file '{attachment_typedid}'") from e

    async def import_files(self, files: dict[str, tuple[Optional[str], bytes | str, str]]) -> str:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(
            f"{self.endpoint}/optimization.modelimport",
            files=files,
        )
        return response.json()["response"]["data"][0]["typedId"]

    async def update_status(
        self,
        jst_id: int,
        status_code: JobStatus,
        progress: Optional[int],
        msg: Optional[str] = None,
        results: Optional[dict[str, Any]] = None,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        data: dict[str, Any] = {
            "jstId": jst_id,
            "status": status_code.value,
            "progress": progress,
        }

        if msg:
            data["calculationMessages"] = [msg]

        if results is not None:
            data["calculationResults"] = [
                {
                    "resultName": name,
                    "resultLabel": name,
                    "result": value,
                    "resultType": "SIMPLE",
                }
                for (name, value) in results.items()
            ]

        await self.session.post(
            f"{self.endpoint}/optimization.updatejst",
            json={"data": data},
        )

    async def query_meta(self, query: dict[str, Any]) -> QueryAnswerMeta:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(
            f"{self.endpoint}/queryapi.meta",
            json={"data": {"query": query}},
        )
        return QueryAnswerMeta.model_validate(response.json()["response"]["data"][0])

    async def query_execute(self, query: dict[str, Any]) -> list[list[Any]]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(
            f"{self.endpoint}/queryapi.execute",
            json={"data": {"query": query}},
        )
        return response.json()["response"]["data"]

    async def create_action(
        self,
        title: str,
        assignee_id: int,
        due_date: str,
        description: str,
        recommendations: Optional[str] = None,
        originator_typed_id: Optional[str] = None,
        dashboard_inputs: Optional[dict[str, Any]] = None,
        dashboard_preferences: Optional[dict[str, Any]] = None,
        action_item_type: str = "__DEFAULT__",
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        dashboard_config = {
            "userInputs": dashboard_inputs,
            "userPreferences": dashboard_preferences,
        }
        response = await self.session.post(
            f"{self.endpoint}/add/AI",
            json={
                "data": {
                    "summary": title,
                    "assignedTo": assignee_id,
                    "originatorTypedId": originator_typed_id,
                    "dueDate": due_date,
                    "description": description,
                    "actionItemType": action_item_type,
                    "parentTypedId": None,
                    "targetContext": (
                        f"""{{"recommendations": {json.dumps(recommendations)}, "quickActionsMatrix": "", "dashboardConfig": {json.dumps(dashboard_config)}}}"""  # noqa: E501
                        if recommendations
                        else None
                    ),
                }
            },
        )
        return response.json()["response"]["data"][0]


class ConnectionLocal(ConnectionAsync):
    """A connection that use the local filesystem.

    This "connection" actually pull/push data from/to the local filesystem.
    Useful debugging or running scripts locally.
    """

    def __init__(
        self,
        path: Path,
        logformat: Optional[str] = None,
    ) -> None:
        self.path = path

        if logformat is not None:
            self._logformat = logformat
        else:
            self._logformat = "%(asctime)s [%(status)s] %(progress)s %(message)s %(results)s"

        self._data_sources_path = self.path / "datasources"
        self.calcitems_path = self.path / "results"
        self._logs_path = self.path / "logs"

        os.makedirs(self._data_sources_path, exist_ok=True)
        os.makedirs(self.calcitems_path, exist_ok=True)
        os.makedirs(self._logs_path, exist_ok=True)

    def __repr__(self) -> str:
        return f"ConnectionLocal({self.path})"

    def _owned_tables_path(self, owner_typedid: str) -> Path:
        os.makedirs(self.path / owner_typedid / "tables", exist_ok=True)
        return self.path / owner_typedid / "tables"

    def _attachments_path(self, model_typedid: str) -> Path:
        os.makedirs(self.path / model_typedid / "attachments", exist_ok=True)
        return self.path / model_typedid / "attachments"

    async def get(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return {"response": {"data": []}}

    async def post(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return {"response": {"data": []}}

    async def login_extended(self) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return {"response": {"data": [{"extendedData": {"Release": "99.0-SNAPSHOT"}}]}}

    async def backend_version(self) -> dict[str, Optional[int]]:
        """See `ConnectionAsync` corresponding method."""
        return {"major": 99, "minor": None, "patch": None}

    async def send_notification(self, notification: Notification) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return {}

    async def list_users(self, criteria: Optional[AdvancedCriteria] = None) -> list[UserInfo]:
        """See `ConnectionAsync` corresponding method."""
        return []

    async def get_application_property(self, property_name: str) -> list[str]:
        """See `ConnectionAsync` corresponding method."""
        return []

    async def get_object(
        self,
        typedid: str,
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method.

        This implementation will return a minimal dict.
        """
        return {"id": self._split_typedid(typedid)[0], "typedId": f"{typedid}"}

    async def list_objects(
        self,
        type_code: str,
        filters: Optional[dict[str, Any] | Sequence[FieldRule] | AdvancedCriteria] = None,
        filter_aggregator: Optional[Operator] = None,
        start_row: Optional[int] = None,
        max_rows: Optional[int] = None,
        sort_by: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method.

        This implementation of the methods always returns an empty list.
        """
        return []

    async def get_object_metadata(
        self,
        type_code: str,
        payload_key: str,
        object_id: int,
    ) -> list[dict[str, Any]]:
        """See `Connection` corresponding method.

        This implementation of the methods always returns an empty list.
        """
        return []

    async def add_object(self, type_code: str, attributes: dict[str, Any]) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method.

        This implementation of the methods always returns the attributes.
        """
        return attributes

    async def update_object(self, type_code: str, attributes: dict[str, Any]) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method.

        This implementation of the methods always returns the attributes.
        """
        return attributes

    async def quick_search(self, sku_or_label: str) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method.

        This implementation of the methods always returns an empty list.
        """
        return []

    async def get_fc(
        self,
        typedid: str,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method.

        This implementation will return a minimal dict.
        """
        return {"typedId": typedid}

    async def list_fcs(
        self,
        type_code: str,
        params: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method.

        This implementation of the methods always returns an empty list.
        """
        return []

    async def create_table(
        self,
        name: str,
        fields_spec: list[dict],
        content: AvroStream,
        label: Optional[str] = None,
        owner_typedid: Optional[str] = None,
        replace_existing: bool = True,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        if owner_typedid is not None:
            tablepath = self._owned_tables_path(owner_typedid) / f"{name}.avro"
        else:
            tablepath = self._data_sources_path / f"{name}.avro"

        with open(tablepath, "wb") as binfile:
            while buffer := binfile.read():
                binfile.write(buffer)

    async def update_table(self, typedid: str, data: AvroStream) -> None:
        """See `ConnectionAsync` corresponding method."""
        filepath = self._data_sources_path / f"{typedid}.avro"
        if not filepath.is_file():
            raise RuntimeError(f"Trying to append to non-existing table '{typedid}'")

        with open(filepath, "ab") as file:
            while buffer := data.read(4096):
                file.write(buffer)

    async def stream_dm_data(
        self,
        typedid: str,
        chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE,
        criteria: Optional[AdvancedCriteria] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> AsyncIterator[bytes]:
        """See `ConnectionAsync` corresponding method.

        `criteria` is not applied: this implementation always streams the full
        local file unfiltered. A warning is logged when `criteria` is set.
        """
        if criteria is not None:
            LOGGER.warning(
                "ConnectionLocal doesn't support server-side row filtering: "
                "streaming the full local file for '%s' unfiltered.",
                typedid,
            )
        content = (
            pd.read_csv(self._data_sources_path / f"{typedid}.csv", usecols=columns)
            .to_csv(index=False)
            .encode()
        )
        for start in range(0, len(content), chunk_size):
            yield content[start : start + chunk_size]

    async def fetch_paginated_dm_data(
        self,
        typedid: str,
        page_size: int = _DEFAULT_PAGE_SIZE,
        criteria: Optional[AdvancedCriteria] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """See `ConnectionAsync` corresponding method.

        `criteria` is not applied: this implementation always reads the full
        local file unfiltered. A warning is logged when `criteria` is set.
        """
        if criteria is not None:
            LOGGER.warning(
                "ConnectionLocal doesn't support server-side row filtering: "
                "reading the full local file for '%s' unfiltered.",
                typedid,
            )
        for chunk in pd.read_csv(
            self._data_sources_path / f"{typedid}.csv",
            chunksize=page_size,
            usecols=columns,
        ):
            yield chunk.to_dict(orient="records")

    async def get_calcitems(self, typedid: str) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        res: list[dict[str, Any]] = []
        with open(self.calcitems_path) as csvfile:
            csvcontent = csv.reader(csvfile)
            header = csvcontent.__next__()
            for row in csvcontent:
                rowdict = {}
                for i, e in enumerate(row):
                    rowdict[header[i]] = e
                res.append(rowdict)
        return res

    async def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """See `ConnectionAsync` corresponding method."""
        file_path = self.calcitems_path / f"{typedid}.csv"
        if not os.path.exists(file_path):
            with open(file_path, "w") as out:
                out.write("key1,key2,value\n")
        with open(file_path, "a") as out:
            csv.writer(out).writerow([key1, key2, value])

    async def list_lpg_items(
        self, lpg_id: int, filters: Optional[dict[str, Any]] = None
    ) -> list[LPGProduct]:
        """See `ConnectionAsync` corresponding method.

        This implementation of the methods always returns an empty list.
        """
        return []

    async def update_lpg(
        self,
        lpg_id: int,
        field_to_update: str,
        new_value: float,
        product: dict[str, Any],
        comment: str,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        return None

    async def submit_lpg(self, lpg_id: int, product_typedids: list[str]) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return []

    async def list_attachments(
        self,
        typedid: str,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        attachments_path = self._attachments_path(typedid)
        res = []
        mimes = mimetypes.MimeTypes()
        for filename in os.listdir(attachments_path):
            filepath = attachments_path / filename
            filestats = os.stat(filepath)
            res.append(
                {
                    "typedId": filename,
                    "fileName": filename,
                    "contentType": mimes.guess_type(str(filepath))[0],
                    "length": os.path.getsize(filepath),
                    "createdBy": filestats.st_uid,
                    "createDate": filestats.st_ctime,
                    "lastUpdateBy": filestats.st_uid,
                    "lastUpdateDate": filestats.st_mtime,
                }
            )
        return res

    async def attach_file(
        self,
        typedid: str,
        name: str,
        content: IO,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        filepath = self._attachments_path(typedid) / name
        if isinstance(content, TextIOBase):
            with open(filepath, "w") as textfile:
                textfile.write(content.read())
        else:
            with open(filepath, "wb") as binfile:
                binfile.write(content.read())

    async def pull_file(
        self,
        owner_typedid: str,
        attachment_typedid: str,
        chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE,
    ) -> AsyncIterator[bytes]:
        """See `ConnectionAsync` corresponding method."""
        with open(self._attachments_path(owner_typedid) / attachment_typedid, "rb") as fin:
            while chunk := fin.read(chunk_size):
                yield chunk

    async def import_files(self, files: dict[str, tuple[Optional[str], bytes | str, str]]) -> str:
        """See `ConnectionAsync` corresponding method."""
        return ""

    async def update_status(
        self,
        jst_id: int,
        status_code: JobStatus,
        progress: Optional[int],
        msg: Optional[str] = None,
        results: Optional[dict[str, Any]] = None,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        logfile = self._logs_path / f"{str(jst_id)}.txt"
        logger = logging.getLogger(str(logfile))
        if len(logger.handlers) == 0:
            loghandler = logging.FileHandler(logfile)
            loghandler.setFormatter(logging.Formatter(self._logformat))
            logger.addHandler(loghandler)
            logger.setLevel(logging.DEBUG)
        if status_code == JobStatus.FAILED:
            log_level = logging.ERROR
        else:
            log_level = logging.INFO
        logger.log(
            log_level,
            msg if msg is not None else "",
            extra={
                "status": status_code.name,
                "progress": str(progress) + "%" if progress is not None else "",
                "results": str(results) if results is not None else "",
            },
        )

    async def query_meta(self, query: dict[str, Any]) -> QueryAnswerMeta:
        """See `ConnectionAsync` corresponding method."""
        return QueryAnswerMeta.model_validate({"columns": []})

    async def query_execute(self, query: dict[str, Any]) -> list[list[Any]]:
        """See `ConnectionAsync` corresponding method."""
        return []

    async def create_action(
        self,
        title: str,
        assignee_id: int,
        due_date: str,
        description: str,
        recommendations: Optional[str] = None,
        originator_typed_id: Optional[str] = None,
        dashboard_inputs: Optional[dict[str, Any]] = None,
        dashboard_preferences: Optional[dict[str, Any]] = None,
        action_item_type: str = "__DEFAULT__",
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return {}


class ConnectionComposed(ConnectionAsync):
    """A connection that composes a remote and local connections.

    This composite connection allows to mix and match calls between a remote
    and local connection according to a given configuration.
    """

    def __init__(
        self,
        dispatch: dict[str, ConnectionAsync],
        default_connection: ConnectionAsync,
    ) -> None:
        self._dispatch = dispatch
        self._default = default_connection

    def __repr__(self) -> str:
        return f"ConnectionDispatch(default={self._default})"

    async def get(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.get(path, **kwargs)

    async def post(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.post(path, **kwargs)

    async def login_extended(self) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.login_extended()

    async def backend_version(self) -> dict[str, Optional[int]]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.backend_version()

    async def send_notification(self, notification: Notification) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.send_notification(notification)

    async def list_users(self, criteria: Optional[AdvancedCriteria] = None) -> list[UserInfo]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.list_users(criteria)

    async def get_application_property(self, property_name: str) -> list[str]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.get_application_property(property_name)

    async def get_object(
        self,
        typedid: str,
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.get_object(typedid)

    async def list_objects(
        self,
        type_code: str,
        filters: Optional[dict[str, Any] | Sequence[FieldRule] | AdvancedCriteria] = None,
        filter_aggregator: Optional[Operator] = None,
        start_row: Optional[int] = None,
        max_rows: Optional[int] = None,
        sort_by: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.list_objects(
            type_code, filters, filter_aggregator, start_row, max_rows, sort_by
        )

    async def get_object_metadata(
        self,
        type_code: str,
        payload_key: str,
        object_id: int,
    ) -> list[dict[str, Any]]:
        """See `Connection` corresponding method."""
        return await self._default.get_object_metadata(type_code, payload_key, object_id)

    async def add_object(self, type_code: str, attributes: dict[str, Any]) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.add_object(type_code, attributes)

    async def update_object(self, type_code: str, attributes: dict[str, Any]) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.update_object(type_code, attributes)

    async def quick_search(self, sku_or_label: str) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.quick_search(sku_or_label)

    async def get_fc(
        self,
        typedid: str,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.get_fc(typedid)

    async def list_fcs(
        self,
        type_code: str,
        params: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.list_fcs(type_code, params)

    async def create_table(
        self,
        name: str,
        fields_spec: list[dict],
        content: AvroStream,
        label: Optional[str] = None,
        owner_typedid: Optional[str] = None,
        replace_existing: bool = True,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        await self._dispatch["model_tables"].create_table(
            name, fields_spec, content, label, owner_typedid, replace_existing
        )

    async def update_table(self, typedid: str, data: AvroStream) -> None:
        """See `ConnectionAsync` corresponding method."""
        await self._dispatch["model_tables"].update_table(typedid, data)

    async def stream_dm_data(
        self,
        typedid: str,
        chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE,
        criteria: Optional[AdvancedCriteria] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> AsyncIterator[bytes]:
        """See `ConnectionAsync` corresponding method."""
        async for chunk in self._dispatch["pa_tables"].stream_dm_data(
            typedid, chunk_size, criteria, columns
        ):
            yield chunk

    async def fetch_paginated_dm_data(
        self,
        typedid: str,
        page_size: int = _DEFAULT_PAGE_SIZE,
        criteria: Optional[AdvancedCriteria] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """See `ConnectionAsync` corresponding method."""
        async for page in self._dispatch["pa_tables"].fetch_paginated_dm_data(
            typedid, page_size, criteria, columns
        ):
            yield page

    async def get_calcitems(self, typedid: str) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return await self._dispatch["model_parameters"].get_calcitems(typedid)

    async def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """See `ConnectionAsync` corresponding method."""
        await self._dispatch["model_parameters"].push_calcitem(typedid, key1, key2, value)

    async def list_lpg_items(
        self, lpg_id: int, filters: Optional[dict[str, Any]] = None
    ) -> list[LPGProduct]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.list_lpg_items(lpg_id, filters)

    async def update_lpg(
        self,
        lpg_id: int,
        field_to_update: str,
        new_value: float,
        product: dict[str, Any],
        comment: str,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.update_lpg(lpg_id, field_to_update, new_value, product, comment)

    async def submit_lpg(self, lpg_id: int, product_typedids: list[str]) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.submit_lpg(lpg_id, product_typedids)

    async def list_attachments(
        self,
        typedid: str,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return await self._dispatch["model_attachments"].list_attachments(typedid)

    async def attach_file(
        self,
        typedid: str,
        name: str,
        content: IO,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        await self._dispatch["model_attachments"].attach_file(typedid, name, content)

    async def pull_file(
        self,
        owner_typedid: str,
        attachment_typedid: str,
        chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE,
    ) -> AsyncIterator[bytes]:
        """See `ConnectionAsync` corresponding method."""
        async for chunk in self._dispatch["model_attachments"].pull_file(
            owner_typedid, attachment_typedid, chunk_size
        ):
            yield chunk

    async def import_files(self, files: dict[str, tuple[Optional[str], bytes | str, str]]) -> str:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.import_files(files)

    async def update_status(
        self,
        jst_id: int,
        status_code: JobStatus,
        progress: Optional[int],
        msg: Optional[str] = None,
        results: Optional[dict[str, Any]] = None,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        await self._dispatch["job_updates"].update_status(
            jst_id, status_code, progress, msg, results
        )

    async def query_meta(self, query: dict[str, Any]) -> QueryAnswerMeta:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.query_meta(query)

    async def query_execute(self, query: dict[str, Any]) -> list[list[Any]]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.query_execute(query)

    async def create_action(
        self,
        title: str,
        assignee_id: int,
        due_date: str,
        description: str,
        recommendations: Optional[str] = None,
        originator_typed_id: Optional[str] = None,
        dashboard_inputs: Optional[dict[str, Any]] = None,
        dashboard_preferences: Optional[dict[str, Any]] = None,
        action_item_type: str = "__DEFAULT__",
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.create_action(
            title,
            assignee_id,
            due_date,
            description,
            recommendations,
            originator_typed_id,
            dashboard_inputs,
            dashboard_preferences,
            action_item_type,
        )


class ConnectionSync:
    """Synchronous wrapper around an async Connection.

    Adapts an async Connection for use in synchronous code (e.g. Python Engine)
    by running each async call through `_run_sync` or `_sync_iterator`.
    """

    def __init__(self, conn: ConnectionAsync):
        self._conn = conn
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        # Disable keep-alive: long-lived sync usage would accumulate idle connections
        # bound to this loop; close after each request to avoid that.
        if hasattr(conn, "session"):
            conn.session.set_header("Connection", "close")

    def __del__(self) -> None:
        try:
            if not self._loop.is_closed():
                self._loop.call_soon_threadsafe(self._loop.stop)  # type: ignore[arg-type]
                self._thread.join(timeout=1)
                self._loop.close()
        except Exception:
            pass

    def __repr__(self) -> str:
        return f"ConnectionSync({self._conn})"

    def _run_sync(self, coro: Coroutine[Any, Any, T]) -> T:
        """Execute an async coroutine synchronously via the dedicated event loop thread."""
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def _sync_iterator(self, async_iter: AsyncIterator[T]) -> Iterator[T]:
        """Convert an async iterator to a sync one using the dedicated event loop thread."""
        while True:
            future = asyncio.run_coroutine_threadsafe(
                cast(Coroutine[Any, Any, T], async_iter.__anext__()), self._loop
            )
            try:
                yield future.result()
            except StopAsyncIteration:
                break

    def get(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.get(path, **kwargs))

    def post(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.post(path, **kwargs))

    def login_extended(self) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.login_extended())

    def backend_version(self) -> dict[str, Optional[int]]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.backend_version())

    def send_notification(self, notification: Notification) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.send_notification(notification))

    def list_users(self, criteria: Optional[AdvancedCriteria] = None) -> list[UserInfo]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.list_users(criteria))

    def get_application_property(self, property_name: str) -> list[str]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.get_application_property(property_name))

    def get_object(
        self,
        typedid: str,
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.get_object(typedid))

    def list_objects(
        self,
        type_code: str,
        filters: Optional[dict[str, Any] | Sequence[FieldRule] | AdvancedCriteria] = None,
        filter_aggregator: Optional[Operator] = None,
        start_row: Optional[int] = None,
        max_rows: Optional[int] = None,
        sort_by: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(
            self._conn.list_objects(
                type_code, filters, filter_aggregator, start_row, max_rows, sort_by
            )
        )

    def get_object_metadata(
        self,
        type_code: str,
        payload_key: str,
        object_id: int,
    ) -> list[dict[str, Any]]:
        """See `Connection` corresponding method."""
        return self._run_sync(self._conn.get_object_metadata(type_code, payload_key, object_id))

    def add_object(self, type_code: str, attributes: dict[str, Any]) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.add_object(type_code, attributes))

    def update_object(self, type_code: str, attributes: dict[str, Any]) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.update_object(type_code, attributes))

    def quick_search(self, sku_or_label: str) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.quick_search(sku_or_label))

    def get_fc(
        self,
        typedid: str,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.get_fc(typedid, params))

    def list_fcs(
        self,
        type_code: str,
        params: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.list_fcs(type_code, params))

    def create_table(
        self,
        name: str,
        fields_spec: list[dict],
        content: AvroStream,
        label: Optional[str] = None,
        owner_typedid: Optional[str] = None,
        replace_existing: bool = True,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(
            self._conn.create_table(
                name, fields_spec, content, label, owner_typedid, replace_existing
            )
        )

    def update_table(self, typedid: str, data: AvroStream) -> None:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.update_table(typedid, data))

    def stream_dm_data(
        self,
        typedid: str,
        chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE,
        criteria: Optional[AdvancedCriteria] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> Iterator[bytes]:
        """See `ConnectionAsync` corresponding method."""
        return self._sync_iterator(
            self._conn.stream_dm_data(typedid, chunk_size, criteria, columns)
        )

    def fetch_paginated_dm_data(
        self,
        typedid: str,
        page_size: int = _DEFAULT_PAGE_SIZE,
        criteria: Optional[AdvancedCriteria] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> Iterator[list[dict[str, Any]]]:
        """See `ConnectionAsync` corresponding method."""
        return self._sync_iterator(
            self._conn.fetch_paginated_dm_data(typedid, page_size, criteria, columns)
        )

    def get_calcitems(self, typedid: str) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.get_calcitems(typedid))

    def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.push_calcitem(typedid, key1, key2, value))

    def list_lpg_items(
        self, lpg_id: int, filters: Optional[dict[str, Any]] = None
    ) -> list[LPGProduct]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.list_lpg_items(lpg_id, filters))

    def update_lpg(
        self,
        lpg_id: int,
        field_to_update: str,
        new_value: float,
        product: dict[str, Any],
        comment: str,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(
            self._conn.update_lpg(lpg_id, field_to_update, new_value, product, comment)
        )

    def submit_lpg(self, lpg_id: int, product_typedids: list[str]) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.submit_lpg(lpg_id, product_typedids))

    def list_attachments(
        self,
        typedid: str,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.list_attachments(typedid))

    def attach_file(
        self,
        typedid: str,
        name: str,
        content: IO,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.attach_file(typedid, name, content))

    def pull_file(
        self,
        owner_typedid: str,
        attachment_typedid: str,
        chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE,
    ) -> Iterator[bytes]:
        """See `ConnectionAsync` corresponding method."""
        return self._sync_iterator(
            self._conn.pull_file(owner_typedid, attachment_typedid, chunk_size)
        )

    def import_files(self, files: dict[str, tuple[Optional[str], bytes | str, str]]) -> str:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.import_files(files))

    def update_status(
        self,
        jst_id: int,
        status_code: JobStatus,
        progress: Optional[int],
        msg: Optional[str] = None,
        results: Optional[dict[str, Any]] = None,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.update_status(jst_id, status_code, progress, msg, results))

    def query_meta(self, query: dict[str, Any]) -> QueryAnswerMeta:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.query_meta(query))

    def query_execute(self, query: dict[str, Any]) -> list[list[Any]]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(self._conn.query_execute(query))

    def create_action(
        self,
        title: str,
        assignee_id: int,
        due_date: str,
        description: str,
        recommendations: Optional[str] = None,
        originator_typed_id: Optional[str] = None,
        dashboard_inputs: Optional[dict[str, Any]] = None,
        dashboard_preferences: Optional[dict[str, Any]] = None,
        action_item_type: str = "__DEFAULT__",
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return self._run_sync(
            self._conn.create_action(
                title,
                assignee_id,
                due_date,
                description,
                recommendations,
                originator_typed_id,
                dashboard_inputs,
                dashboard_preferences,
                action_item_type,
            )
        )
