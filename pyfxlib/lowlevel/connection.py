"""Interacting with a Pricefx platform directly.

This platform defines a low-level wrapper to interact with the Pricefx platform.
For a higher level API, see the `pyfxlib.api.domain` package.

"""

from abc import ABC, abstractmethod
import asyncio
from collections.abc import AsyncIterator, Coroutine, Iterator
import csv
from enum import Enum, StrEnum, unique
from io import BytesIO, TextIOBase
import json
import logging
import mimetypes
import os
from pathlib import Path
import re
from typing import Any, IO, TypeVar

from httpx import HTTPStatusError
import pandas as pd

from pyfxlib.lowlevel import _DEFAULT_PAGE_SIZE, _DEFAULT_STREAM_CHUNK_SIZE
from pyfxlib.lowlevel.avro import AvroStream
from pyfxlib.lowlevel.session import PfxSession

LOGGER = logging.getLogger(__name__)


@unique
class JobStatus(Enum):
    """Possible status of a job for the Pricefx REST API."""

    PROCESSING = "PROCESSING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class ConnectionAsync(ABC):
    """Abstract class for an async Connection.

    A connection abstract the low-lever details of interacting with a "backend".
    This backend can be an actual Pricefx instance, or can be substituted with,
    e.g. a local mock.
    """

    @abstractmethod
    async def backend_version(self) -> dict[str, int | None]:
        """Fetch the backend version information.

        Returns:
            A dict containing at least the major version,
            optionally the minor and the patch values,
             e.g. {"major": 15, "minor": 2, "patch": 0}.
        """
        pass

    @abstractmethod
    async def update_status(
        self,
        jst_id: int,
        status_code: JobStatus,
        progress: int | None,
        msg: str | None = None,
        results: dict[str, Any] | None = None,
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
    async def get_fcs(
        self,
        typedid: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Get the attributes of a specific fields collection."""
        pass

    @abstractmethod
    async def list_fcs(
        self,
        type_code: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """List all the fields collections of a given type."""
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
    async def quick_search(self, sku_or_label: str) -> list[dict[str, Any]]:
        """Search for all lists containing a product designated by its sku or label.

        Note that the search is case-sensitive.
        """
        pass

    @abstractmethod
    async def list_objects(
        self,
        type_code: str,
        filters: list[dict[str, Any]] | None = None,
        filter_aggregator: str | None = None,
        start_row: int | None = None,
        max_rows: int | None = None,
        sort_by: str | None = None,
    ) -> list[dict[str, Any]]:
        """List all the elements of a given type with optional filtering and pagination.

        Common types: P (Products), C (Customers), PL (Price Lists), Q (Quotes),
                      MO (Model Objects), DMM (DM Models), DMT (DM Tables),
                      DMDS (DM Data Sources)...
        If you need the full list of TypeCodes, search the knowledge base for "Type Codes".

        Args:
            type_code: Entity TypeCode
            filters: a list of filters in the format
                     [{"fieldName": ..., "operator": ..., "value": ...}, ...]
            filter_aggregator: "and" or "or" (default) when multiple filter values are provided
            start_row: Starting index (0-based)
            max_rows: Max results (1-200, default 5)
            sort: field on which to sort the data

        Returns:
            List of entity objects with all fields,
            as defined in the Pricefx REST API public documentation.
        """
        pass

    @abstractmethod
    async def list_lpg_items(
        self, lpg_id: int, filters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
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
    async def stream_fcs(
        self, typedid: str, chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE
    ) -> AsyncIterator[bytes]:
        """Stream the content of a data source."""
        yield b""

    @abstractmethod
    async def fetch_paginated_fcs(
        self, typedid: str, page_size: int = _DEFAULT_PAGE_SIZE
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Fetch the content of a data source using paginated requests.

        More reliable than stream_fcs for large datasets.
        """
        yield []

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
    async def create_table(
        self,
        name: str,
        fields_spec: list[dict],
        content: AvroStream,
        label: str | None = None,
        owner_typedid: str | None = None,
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
    async def get_calcitems(self, typedid: str) -> list[dict[str, Any]]:
        """Get the calculation items associated with a specific typedid."""
        pass

    @abstractmethod
    async def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """Push a new calculation item associated with a specific typedid."""
        pass

    @abstractmethod
    async def send_notification(self, notification: dict[str, Any]) -> dict[str, Any]:
        """Send a notification to the user via the backend.

        See https://pricefx.atlassian.net/wiki/spaces/UNITY/pages/5482283105/App+Notifications
        and https://api.pricefx.com/openapi/reference/pricefx-server_openapi/notifications/post-notification.send # noqa: E501
        for details on allowed values in the request body.

        Returns the backend response as a dictionary.
        Notifications are not supported by the BE versions < 15.2 (raises an error)
        """
        pass


# By default, stream download timeout is 60s which may cause errors when the table is large.
# (see https://pricefx.atlassian.net/browse/PFUN-14665)
# When the back end receives a timeout, it chooses an actual timeout which is the minimum value
# between a configured "MAX TIMEOUT" value and the timeout argument.
# The solution here is to set the timeout to 1h so that the back end will always choose this
# configured "MAX TIMEOUT" value up to 1h.
_DATAMART_FETCH_TIMEOUT = 3600


# TODO: copy from genfx, remove duplication
class FilterOperator(StrEnum):
    """Filter operators for search criteria."""

    EQUALS = "equals"
    IEQUALS = "iEquals"
    NOTEQUAL = "notEqual"
    INOTEQUAL = "iNotEqual"
    GREATERTHAN = "greaterThan"
    GREATEROREQUAL = "greaterOrEqual"
    LESSOREQUAL = "lessOrEqual"
    LESSTHAN = "lessThan"
    ISNULL = "isNull"
    NOTNULL = "notNull"
    CONTAINS = "contains"
    ICONTAINS = "iContains"
    CONTAINSPATTERN = "containsPattern"
    ICONTAINSPATTERN = "iContainsPattern"
    NOTCONTAINS = "notContains"
    INOTCONTAINS = "iNotContains"
    STARTSWITH = "startsWith"
    ISTARTSWITH = "iStartsWith"
    NOTSTARTSWITH = "notStartsWith"
    INOTSTARTSWITH = "iNotStartsWith"
    ENDSWITH = "endsWith"
    IENDSWITH = "iEndsWith"
    NOTENDSWITH = "notEndsWith"
    INOTENDSWITH = "iNotEndsWith"
    BETWEEN = "between"
    BETWEENINCLUSIVE = "betweenInclusive"
    IBETWEEN = "iBetween"
    IBETWEENINCLUSIVE = "iBetweenInclusive"
    INSET = "inSet"
    NOTINSET = "notInSet"


class ConnectionRemote(ConnectionAsync):
    """A connection to a remote instance.

    This class abstracts the various aspects of interacting with the Pricefx platform behind an
    unified interface.
    """

    def __init__(self, endpoint: str, pfxsession: PfxSession) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.session = pfxsession

    def __repr__(self) -> str:
        return f"ConnectionRemote({self.endpoint})"

    async def backend_version(self) -> dict[str, int | None]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(f"{self.endpoint}/login/extended")
        backend_version = str(response.json()["response"]["data"][0]["extendedData"]["Release"])
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

    async def update_status(
        self,
        jst_id: int,
        status_code: JobStatus,
        progress: int | None,
        msg: str | None = None,
        results: dict[str, Any] | None = None,
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

    async def get_fcs(self, typedid: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(
            f"{self.endpoint}/datamart.getfcs/{typedid}",
            json={"data": params},
        )
        return response.json()["response"]["data"][0]

    async def list_fcs(
        self, type_code: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(
            f"{self.endpoint}/datamart.getfcs/{type_code}", json={"data": params}
        )
        return response.json()["response"]["data"]

    async def get_object(
        self,
        typedid: str,
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        id, type_code = _split_typedid(typedid)
        response = await self.session.post(f"{self.endpoint}/fetch/{type_code}/{id}")
        data = response.json()["response"]["data"]
        if data is not None:
            return data[0]
        else:
            raise ValueError(f"Object with typedId '{typedid}' not found")

    async def quick_search(self, sku_or_label: str) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        response = await self.session.post(
            f"{self.endpoint}/productmanager.quicksearch/{sku_or_label}"
        )
        return response.json()["response"]["data"]

    @staticmethod
    def _build_criteria(
        filters: list[dict[str, Any]] | None = None,
        filter_aggregator: str | None = None,
    ) -> dict[str, Any] | None:
        if not filters:
            return None
        for filt in filters:
            if not {"fieldName", "operator", "value"}.issubset(filt.keys()):
                raise ValueError(
                    f"Invalid filter format: {filt}. "
                    "Expected keys: 'fieldName', 'operator', 'value'."
                )
            if filt["operator"] not in FilterOperator:
                raise ValueError(
                    f"Invalid filter operator for the filter {filt}. "
                    f"Expected one of: {[operator.value for operator in FilterOperator]}"
                )
        return {
            "_constructor": "AdvancedCriteria",
            "operator": filter_aggregator if filter_aggregator else "or",
            "criteria": filters,
        }

    async def list_objects(
        self,
        type_code: str,
        filters: list[dict[str, Any]] | None = None,
        filter_aggregator: str | None = None,
        start_row: int | None = None,
        max_rows: int | None = None,
        sort_by: str | None = None,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        start_row = start_row or 0
        end_row = start_row + max(min(max_rows, 200), 1) if max_rows is not None else None
        body = {
            "startRow": start_row,
            "endRow": end_row,
            "operationType": "fetch",
            "textMatchStyle": "exact",
            "data": self._build_criteria(filters, filter_aggregator),
            "sortBy": [sort_by] if sort_by else None,
        }
        response = await self.session.post(f"{self.endpoint}/fetch/{type_code}", json=body)
        return response.json()["response"]["data"]

    async def list_lpg_items(
        self, lpg_id: int, filters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        criteria = []
        if filters:
            criteria = [
                {"fieldName": key, "operator": FilterOperator.IEQUALS, "value": value}
                for key, value in filters.items()
            ]
        response = await self.session.post(
            f"{self.endpoint}/pricegridmanager.fetch/{lpg_id}",
            json={"data": self._build_criteria(criteria, "and")},
        )
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

    async def stream_fcs(
        self, typedid: str, chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE
    ) -> AsyncIterator[bytes]:
        """See `ConnectionAsync` corresponding method."""
        async for chunk in self.session.get_stream(
            f"{self.endpoint}/datamart.fetch/{typedid}?stream&timeout={_DATAMART_FETCH_TIMEOUT}",
            chunk_size=chunk_size,
            params={"output": "csv"},
        ):
            yield chunk

    async def fetch_paginated_fcs(
        self, typedid: str, page_size: int = _DEFAULT_PAGE_SIZE
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """See `ConnectionAsync` corresponding method."""
        sort_by: list[str] = []
        fc_meta = await self.get_fcs(typedid)
        sort_by = [field["name"] for field in fc_meta.get("fields", []) if field.get("key", False)]

        start_row = 0
        while True:
            response = await self.session.post(
                f"{self.endpoint}/datamart.fetch/{typedid}",
                json={"startRow": start_row, "endRow": start_row + page_size, "sortBy": sort_by},
            )
            data = response.json()["response"]["data"]
            if not data:
                break
            yield data
            start_row += page_size

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

    async def _delete_fc(self, fc_type: str, name: str) -> None:
        await self.session.post(
            f"{self.endpoint}/datamart.deletefc/{fc_type}",
            json={"data": {"uniqueName": name}},
        )

    async def create_table(
        self,
        name: str,
        fields_spec: list[dict],
        content: AvroStream,
        label: str | None = None,
        owner_typedid: str | None = None,
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

    async def _fc_spec(self, typedid: str) -> dict | None:
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

    async def get_calcitems(self, typedid: str) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        url = f"{self.endpoint}/lookuptablemanager.fetch/{_split_typedid(typedid)[0]}"
        response = await self.session.post(url)
        return response.json()["response"]["data"]

    async def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """See `ConnectionAsync` corresponding method."""
        url = f"{self.endpoint}/lookuptablemanager.add/{_split_typedid(typedid)[0]}"
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

    async def send_notification(self, notification: dict[str, Any]) -> dict[str, Any]:
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
            json={"data": {"notification": notification}},
        )
        return response.json()["response"]


def _split_typedid(typed_id: str) -> tuple[int, str]:
    match = re.search(r"^(?P<id>[0-9]+)\.(?P<type_code>[A-Z]+)$", typed_id)
    if match:
        return (int(match.group("id")), match.group("type_code"))
    raise ValueError(f"'{typed_id}' is not a valid typedId")


class ConnectionLocal(ConnectionAsync):
    """A connection that use the local filesystem.

    This "connection" actually pull/push data from/to the local filesystem.
    Useful debugging or running scripts locally.
    """

    def __init__(
        self,
        path: Path,
        logformat: str | None = None,
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

    async def backend_version(self) -> dict[str, int | None]:
        """See `ConnectionAsync` corresponding method."""
        return {"major": 99, "minor": None, "patch": None}

    async def update_status(
        self,
        jst_id: int,
        status_code: JobStatus,
        progress: int | None,
        msg: str | None = None,
        results: dict[str, Any] | None = None,
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

    async def get_fcs(
        self,
        typedid: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method.

        This implementation will return a minimal dict.
        """
        return {"typedId": typedid}

    async def list_fcs(
        self,
        type_code: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method.

        This implementation of the methods always returns an empty list.
        """
        return []

    async def get_object(
        self,
        typedid: str,
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method.

        This implementation will return a minimal dict.
        """
        return {"id": _split_typedid(typedid)[0], "typedId": f"{typedid}"}

    async def quick_search(self, sku_or_label: str) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method.

        This implementation of the methods always returns an empty list.
        """
        return []

    async def list_objects(
        self,
        type_code: str,
        filters: list[dict[str, Any]] | None = None,
        filter_aggregator: str | None = None,
        start_row: int | None = None,
        max_rows: int | None = None,
        sort_by: str | None = None,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method.

        This implementation of the methods always returns an empty list.
        """
        return []

    async def list_lpg_items(
        self, lpg_id: int, filters: dict[str, Any] | None = None
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

    async def stream_fcs(
        self, typedid: str, chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE
    ) -> AsyncIterator[bytes]:
        """See `ConnectionAsync` corresponding method."""
        with open(self._data_sources_path / f"{typedid}.csv", "rb") as fin:
            while chunk := fin.read(chunk_size):
                yield chunk

    async def fetch_paginated_fcs(
        self, typedid: str, page_size: int = _DEFAULT_PAGE_SIZE
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """See `ConnectionAsync` corresponding method."""
        for chunk in pd.read_csv(self._data_sources_path / f"{typedid}.csv", chunksize=page_size):
            yield chunk.to_dict(orient="records")

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

    async def create_table(
        self,
        name: str,
        fields_spec: list[dict],
        content: AvroStream,
        label: str | None = None,
        owner_typedid: str | None = None,
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

    async def _pull_dir(self, dir_type: str, dir_path: str) -> None:
        # Nothing to do
        return

    async def _push_result_files(self, result_dir_path: str) -> None:
        # Nothing to do
        return

    async def send_notification(self, notification: dict[str, Any]) -> dict[str, Any]:
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

    async def backend_version(self) -> dict[str, int | None]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.backend_version()

    async def update_status(
        self,
        jst_id: int,
        status_code: JobStatus,
        progress: int | None,
        msg: str | None = None,
        results: dict[str, Any] | None = None,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        await self._dispatch["job_updates"].update_status(
            jst_id, status_code, progress, msg, results
        )

    async def get_fcs(
        self,
        typedid: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.get_fcs(typedid)

    async def list_fcs(
        self,
        type_code: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.list_fcs(type_code, params)

    async def get_object(
        self,
        typedid: str,
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.get_object(typedid)

    async def quick_search(self, sku_or_label: str) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.quick_search(sku_or_label)

    async def list_objects(
        self,
        type_code: str,
        filters: list[dict[str, Any]] | None = None,
        filter_aggregator: str | None = None,
        start_row: int | None = None,
        max_rows: int | None = None,
        sort_by: str | None = None,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.list_objects(
            type_code, filters, filter_aggregator, start_row, max_rows, sort_by
        )

    async def list_lpg_items(
        self, lpg_id: int, filters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.list_lpg_items(lpg_id, filters)

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

    async def stream_fcs(
        self, typedid: str, chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE
    ) -> AsyncIterator[bytes]:
        """See `ConnectionAsync` corresponding method."""
        async for chunk in self._dispatch["pa_tables"].stream_fcs(typedid, chunk_size):
            yield chunk

    async def fetch_paginated_fcs(
        self, typedid: str, page_size: int = _DEFAULT_PAGE_SIZE
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """See `ConnectionAsync` corresponding method."""
        async for page in self._dispatch["pa_tables"].fetch_paginated_fcs(typedid, page_size):
            yield page

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

    async def create_table(
        self,
        name: str,
        fields_spec: list[dict],
        content: AvroStream,
        label: str | None = None,
        owner_typedid: str | None = None,
        replace_existing: bool = True,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        await self._dispatch["model_tables"].create_table(
            name, fields_spec, content, label, owner_typedid, replace_existing
        )

    async def update_table(self, typedid: str, data: AvroStream) -> None:
        """See `ConnectionAsync` corresponding method."""
        await self._dispatch["model_tables"].update_table(typedid, data)

    async def get_calcitems(self, typedid: str) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return await self._dispatch["model_parameters"].get_calcitems(typedid)

    async def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """See `ConnectionAsync` corresponding method."""
        await self._dispatch["model_parameters"].push_calcitem(typedid, key1, key2, value)

    async def send_notification(self, notification: dict[str, Any]) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return await self._default.send_notification(notification)


T = TypeVar("T")


def _run_sync(coro: Coroutine[Any, Any, T]) -> T:
    """Execute an async coroutine synchronously."""
    return asyncio.run(coro)


def _sync_iterator(async_iter: AsyncIterator[T]) -> Iterator[T]:
    """Convert an async iterator to a sync one."""
    loop = asyncio.new_event_loop()
    try:
        while True:
            try:
                yield loop.run_until_complete(async_iter.__anext__())
            except StopAsyncIteration:
                break
    finally:
        loop.close()


class ConnectionSync:
    """Synchronous wrapper around an async Connection.

    Adapts an async Connection for use in synchronous code (e.g. Python Engine)
    by running each async call through `_run_sync` or `_sync_iterator`.
    """

    def __init__(self, conn: ConnectionAsync):
        self._conn = conn
        # Disable keep-alive: close is needed for sync usage because asyncio.run() creates/destroys
        # the event loop on each call, making keep-alive connections try to reuse a closed
        # event loop.
        if hasattr(conn, "session"):
            conn.session.set_header("Connection", "close")

    def backend_version(self) -> dict[str, int | None]:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(self._conn.backend_version())

    def update_status(
        self,
        jst_id: int,
        status_code: JobStatus,
        progress: int | None,
        msg: str | None = None,
        results: dict[str, Any] | None = None,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(self._conn.update_status(jst_id, status_code, progress, msg, results))

    def get_fcs(
        self,
        typedid: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(self._conn.get_fcs(typedid, params))

    def list_fcs(
        self,
        type_code: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(self._conn.list_fcs(type_code, params))

    def get_object(
        self,
        typedid: str,
    ) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(self._conn.get_object(typedid))

    def quick_search(self, sku_or_label: str) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(self._conn.quick_search(sku_or_label))

    def list_objects(
        self,
        type_code: str,
        filters: list[dict[str, Any]] | None = None,
        filter_aggregator: str | None = None,
        start_row: int | None = None,
        max_rows: int | None = None,
        sort_by: str | None = None,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(
            self._conn.list_objects(
                type_code, filters, filter_aggregator, start_row, max_rows, sort_by
            )
        )

    def list_lpg_items(
        self, lpg_id: int, filters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(self._conn.list_lpg_items(lpg_id, filters))

    def get_object_metadata(
        self,
        type_code: str,
        payload_key: str,
        object_id: int,
    ) -> list[dict[str, Any]]:
        """See `Connection` corresponding method."""
        return _run_sync(self._conn.get_object_metadata(type_code, payload_key, object_id))

    def add_object(self, type_code: str, attributes: dict[str, Any]) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(self._conn.add_object(type_code, attributes))

    def update_object(self, type_code: str, attributes: dict[str, Any]) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(self._conn.update_object(type_code, attributes))

    def stream_fcs(
        self, typedid: str, chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE
    ) -> Iterator[bytes]:
        """See `ConnectionAsync` corresponding method."""
        return _sync_iterator(self._conn.stream_fcs(typedid, chunk_size))

    def fetch_paginated_fcs(
        self, typedid: str, page_size: int = _DEFAULT_PAGE_SIZE
    ) -> Iterator[list[dict[str, Any]]]:
        """See `ConnectionAsync` corresponding method."""
        return _sync_iterator(self._conn.fetch_paginated_fcs(typedid, page_size))

    def list_attachments(
        self,
        typedid: str,
    ) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(self._conn.list_attachments(typedid))

    def attach_file(
        self,
        typedid: str,
        name: str,
        content: IO,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(self._conn.attach_file(typedid, name, content))

    def pull_file(
        self,
        owner_typedid: str,
        attachment_typedid: str,
        chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE,
    ) -> Iterator[bytes]:
        """See `ConnectionAsync` corresponding method."""
        return _sync_iterator(self._conn.pull_file(owner_typedid, attachment_typedid, chunk_size))

    def create_table(
        self,
        name: str,
        fields_spec: list[dict],
        content: AvroStream,
        label: str | None = None,
        owner_typedid: str | None = None,
        replace_existing: bool = True,
    ) -> None:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(
            self._conn.create_table(
                name, fields_spec, content, label, owner_typedid, replace_existing
            )
        )

    def update_table(self, typedid: str, data: AvroStream) -> None:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(self._conn.update_table(typedid, data))

    def get_calcitems(self, typedid: str) -> list[dict[str, Any]]:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(self._conn.get_calcitems(typedid))

    def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(self._conn.push_calcitem(typedid, key1, key2, value))

    def send_notification(self, notification: dict[str, Any]) -> dict[str, Any]:
        """See `ConnectionAsync` corresponding method."""
        return _run_sync(self._conn.send_notification(notification))
