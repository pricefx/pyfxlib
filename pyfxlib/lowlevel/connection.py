"""Interacting with a Pricefx platform directly.

This platform defines a low-level wrapper to interact with the Pricefx platform.
For a higher level API, see the `pyfxlib.api.domain` package.

"""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
import csv
from enum import Enum, unique
from io import BytesIO, TextIOBase
import json
import logging
import mimetypes
import os
from pathlib import Path
import re
from typing import Any, Dict, IO, List, Optional, Tuple

from httpx import HTTPStatusError

from pyfxlib.lowlevel.avro import AvroStream
from pyfxlib.lowlevel.session import PfxSession

LOGGER = logging.getLogger(__name__)


@unique
class JobStatus(Enum):
    """Possible status of a job for the Pricefx REST API."""

    PROCESSING = "PROCESSING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class Connection(ABC):
    """Abstract class for a Connection.

    A connection abstract the low-lever details of interacting with a "backend".
    This backend can be an actual Pricefx instance, or can be substituted with,
    e.g. a local mock.
    """

    @abstractmethod
    async def update_status(
        self,
        jst_id: int,
        status_code: JobStatus,
        progress: Optional[int],
        msg: Optional[str] = None,
        results: Optional[Dict[str, Any]] = None,
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
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Get the attributes of a specific fields collection."""
        pass

    @abstractmethod
    async def list_fcs(
        self,
        type_code: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """List all the fields collections of a given type."""
        pass

    @abstractmethod
    async def get_object(
        self,
        typedid: str,
    ) -> Dict[str, Any]:
        """Get the attributes of a specific element."""
        pass

    @abstractmethod
    async def list_objects(
        self,
        type_code: str,
    ) -> List[Dict[str, Any]]:
        """List all the elements of a given type."""
        pass

    @abstractmethod
    async def add_object(
        self,
        type_code: str,
        attributes: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Add an object and returns its actual attributes."""
        pass

    @abstractmethod
    async def update_object(
        self,
        type_code: str,
        attributes: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Update an object and returns its actual attributes."""
        pass

    @abstractmethod
    async def stream_fcs(self, typedid: str, chunk_size: int = 128) -> AsyncIterator[bytes]:
        """Stream the content of a data source."""
        yield b""

    @abstractmethod
    async def list_attachments(
        self,
        typedid: str,
    ) -> List[Dict[str, Any]]:
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
        self, owner_typedid: str, attachment_typedid: str, chunk_size: int = 128
    ) -> AsyncIterator[bytes]:
        """Fetch file from the backend."""
        yield b""

    @abstractmethod
    async def create_table(
        self,
        name: str,
        fields_spec: List[Dict],
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
    async def get_calcitems(self, typedid: str) -> List[Dict[str, Any]]:
        """Get the calculation items associated with a specific typedid."""
        pass

    @abstractmethod
    async def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """Push a new calculation item associated with a specific typedid."""
        pass


# By default, stream download timeout is 60s which may cause errors when the table is large.
# (see https://pricefx.atlassian.net/browse/PFUN-14665)
# When the back end receives a timeout, it chooses an actual timeout which is the minimum value
# between a configured "MAX TIMEOUT" value and the timeout argument.
# The solution here is to set the timeout to 1h so that the back end will always choose this
# configured "MAX TIMEOUT" value up to 1h.
_DATAMART_FETCH_TIMEOUT = 3600


class ConnectionRemote(Connection):
    """A connection to a remote instance.

    This class abstracts the various aspects of interacting with the Pricefx platform behind an
    unified interface.
    """

    def __init__(self, endpoint: str, pfxsession: PfxSession) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.session = pfxsession

    def __repr__(self) -> str:
        return f"ConnectionRemote({self.endpoint})"

    async def update_status(
        self,
        jst_id: int,
        status_code: JobStatus,
        progress: Optional[int],
        msg: Optional[str] = None,
        results: Optional[Dict[str, Any]] = None,
    ) -> None:
        """See `Connection` corresponding method."""
        data: Dict[str, Any] = {
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

    async def get_fcs(
        self, typedid: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        response = await self.session.post(
            f"{self.endpoint}/datamart.getfcs/{typedid}",
            json={"data": params},
        )
        return response.json()["response"]["data"][0]

    async def list_fcs(
        self, type_code: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        response = await self.session.post(
            f"{self.endpoint}/datamart.getfcs/{type_code}", json={"data": params}
        )
        return response.json()["response"]["data"]

    async def get_object(
        self,
        typedid: str,
    ) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        id, type_code = _split_typedid(typedid)
        response = await self.session.post(f"{self.endpoint}/fetch/{type_code}/{id}")
        return response.json()["response"]["data"][0]

    async def list_objects(
        self,
        type_code: str,
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        response = await self.session.post(f"{self.endpoint}/fetch/{type_code}")
        return response.json()["response"]["data"]

    async def add_object(self, type_code: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        response = await self.session.post(
            f"{self.endpoint}/add/{type_code}",
            json={"data": attributes},
        )
        return response.json()["response"]["data"][0]

    async def update_object(self, type_code: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        response = await self.session.post(
            f"{self.endpoint}/update/{type_code}",
            json={"data": attributes},
        )
        return response.json()["response"]["data"][0]

    async def stream_fcs(self, typedid: str, chunk_size: int = 128) -> AsyncIterator[bytes]:
        """See `Connection` corresponding method."""
        async for chunk in self.session.get_stream(
            f"{self.endpoint}/datamart.fetch/{typedid}?stream&timeout={_DATAMART_FETCH_TIMEOUT}",
            chunk_size=chunk_size,
            params={"output": "csv"},
        ):
            yield chunk

    async def list_attachments(
        self,
        typedid: str,
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        response = await self.session.post(f"{self.endpoint}/bdmanager.list/{typedid}")
        return response.json()["response"]["data"]

    async def attach_file(
        self,
        typedid: str,
        name: str,
        content: IO,
    ) -> None:
        """See `Connection` corresponding method."""
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
        self, owner_typedid: str, attachment_typedid: str, chunk_size: int = 128
    ) -> AsyncIterator[bytes]:
        """See `Connection` corresponding method."""
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
        fields_spec: List[Dict],
        content: AvroStream,
        label: Optional[str] = None,
        owner_typedid: Optional[str] = None,
        replace_existing: bool = True,
    ) -> None:
        """See `Connection` corresponding method."""
        typecode = "DMT" if owner_typedid is not None else "DMDS"

        schema: Dict[str, Any] = {
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

    async def _fc_spec(self, typedid: str) -> Optional[Dict]:
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
        """See `Connection` corresponding method."""
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

    async def get_calcitems(self, typedid: str) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        url = f"{self.endpoint}/lookuptablemanager.fetch/{_split_typedid(typedid)[0]}"
        response = await self.session.post(url)
        return response.json()["response"]["data"]

    async def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """See `Connection` corresponding method."""
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


def _split_typedid(typed_id: str) -> Tuple[int, str]:
    match = re.search(r"^(?P<id>[0-9]+)\.(?P<type_code>[A-Z]+)$", typed_id)
    if match:
        return (int(match.group("id")), match.group("type_code"))
    raise ValueError(f"'{typed_id}' is not a valid typedId")


class ConnectionLocal(Connection):
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

    async def update_status(
        self,
        jst_id: int,
        status_code: JobStatus,
        progress: Optional[int],
        msg: Optional[str] = None,
        results: Optional[Dict[str, Any]] = None,
    ) -> None:
        """See `Connection` corresponding method."""
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
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """See `Connection` corresponding method.

        This implementation will return a minimal dict.
        """
        return {"typedId": typedid}

    async def list_fcs(
        self,
        type_code: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method.

        This implementation of the methods always returns an empty list.
        """
        return []

    async def get_object(
        self,
        typedid: str,
    ) -> Dict[str, Any]:
        """See `Connection` corresponding method.

        This implementation will return a minimal dict.
        """
        return {"id": _split_typedid(typedid)[0], "typedId": f"{typedid}"}

    async def list_objects(
        self,
        type_code: str,
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method.

        This implementation of the methods always returns an empty list.
        """
        return []

    async def add_object(self, type_code: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """See `Connection` corresponding method.

        This implementation of the methods always returns the attributes.
        """
        return attributes

    async def update_object(self, type_code: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """See `Connection` corresponding method.

        This implementation of the methods always returns the attributes.
        """
        return attributes

    async def stream_fcs(self, typedid: str, chunk_size: int = 128) -> AsyncIterator[bytes]:
        """See `Connection` corresponding method."""
        with open(self._data_sources_path / f"{typedid}.csv", "rb") as fin:
            while chunk := fin.read(chunk_size):
                yield chunk

    async def list_attachments(
        self,
        typedid: str,
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
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
        """See `Connection` corresponding method."""
        filepath = self._attachments_path(typedid) / name
        if isinstance(content, TextIOBase):
            with open(filepath, "w") as textfile:
                textfile.write(content.read())
        else:
            with open(filepath, "wb") as binfile:
                binfile.write(content.read())

    async def pull_file(
        self, owner_typedid: str, attachment_typedid: str, chunk_size: int = 128
    ) -> AsyncIterator[bytes]:
        """See `Connection` corresponding method."""
        with open(self._attachments_path(owner_typedid) / attachment_typedid, "rb") as fin:
            while chunk := fin.read(chunk_size):
                yield chunk

    async def create_table(
        self,
        name: str,
        fields_spec: List[Dict],
        content: AvroStream,
        label: Optional[str] = None,
        owner_typedid: Optional[str] = None,
        replace_existing: bool = True,
    ) -> None:
        """See `Connection` corresponding method."""
        if owner_typedid is not None:
            tablepath = self._owned_tables_path(owner_typedid) / f"{name}.avro"
        else:
            tablepath = self._data_sources_path / f"{name}.avro"

        with open(tablepath, "wb") as binfile:
            while buffer := binfile.read():
                binfile.write(buffer)

    async def update_table(self, typedid: str, data: AvroStream) -> None:
        """See `Connection` corresponding method."""
        filepath = self._data_sources_path / f"{typedid}.avro"
        if not filepath.is_file():
            raise RuntimeError(f"Trying to append to non-existing table '{typedid}'")

        with open(filepath, "ab") as file:
            while buffer := data.read(4096):
                file.write(buffer)

    async def get_calcitems(self, typedid: str) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        res: List[Dict[str, Any]] = []
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
        """See `Connection` corresponding method."""
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


class ConnectionComposed(Connection):
    """A connection that composes a remote and local connections.

    This composite connection allows to mix and match calls between a remote
    and local connection according to a given configuration.
    """

    def __init__(
        self,
        dispatch: Dict[str, Connection],
        default_connection: Connection,
    ) -> None:
        self._dispatch = dispatch
        self._default = default_connection

    def __repr__(self) -> str:
        return f"ConnectionDispatch(default={self._default})"

    async def update_status(
        self,
        jst_id: int,
        status_code: JobStatus,
        progress: Optional[int],
        msg: Optional[str] = None,
        results: Optional[Dict[str, Any]] = None,
    ) -> None:
        """See `Connection` corresponding method."""
        await self._dispatch["job_updates"].update_status(
            jst_id, status_code, progress, msg, results
        )

    async def get_fcs(
        self,
        typedid: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        return await self._default.get_fcs(typedid)

    async def list_fcs(
        self,
        type_code: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        return await self._default.list_fcs(type_code, params)

    async def get_object(
        self,
        typedid: str,
    ) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        return await self._default.get_object(typedid)

    async def list_objects(
        self,
        type_code: str,
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        return await self._default.list_objects(type_code)

    async def add_object(self, type_code: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        return await self._default.add_object(type_code, attributes)

    async def update_object(self, type_code: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        return await self._default.update_object(type_code, attributes)

    async def stream_fcs(self, typedid: str, chunk_size: int = 128) -> AsyncIterator[bytes]:
        """See `Connection` corresponding method."""
        async for chunk in self._dispatch["pa_tables"].stream_fcs(typedid, chunk_size):
            yield chunk

    async def list_attachments(
        self,
        typedid: str,
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        return await self._dispatch["model_attachments"].list_attachments(typedid)

    async def attach_file(
        self,
        typedid: str,
        name: str,
        content: IO,
    ) -> None:
        """See `Connection` corresponding method."""
        await self._dispatch["model_attachments"].attach_file(typedid, name, content)

    async def pull_file(
        self, owner_typedid: str, attachment_typedid: str, chunk_size: int = 128
    ) -> AsyncIterator[bytes]:
        """See `Connection` corresponding method."""
        async for chunk in self._dispatch["model_attachments"].pull_file(
            owner_typedid, attachment_typedid, chunk_size
        ):
            yield chunk

    async def create_table(
        self,
        name: str,
        fields_spec: List[Dict],
        content: AvroStream,
        label: Optional[str] = None,
        owner_typedid: Optional[str] = None,
        replace_existing: bool = True,
    ) -> None:
        """See `Connection` corresponding method."""
        await self._dispatch["model_tables"].create_table(
            name, fields_spec, content, label, owner_typedid, replace_existing
        )

    async def update_table(self, typedid: str, data: AvroStream) -> None:
        """See `Connection` corresponding method."""
        await self._dispatch["model_tables"].update_table(typedid, data)

    async def get_calcitems(self, typedid: str) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        return await self._dispatch["model_parameters"].get_calcitems(typedid)

    async def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """See `Connection` corresponding method."""
        await self._dispatch["model_parameters"].push_calcitem(typedid, key1, key2, value)
