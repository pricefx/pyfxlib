"""Interacting with a Pricefx platform directly.

This platform defines a low-level wrapper to interact with the Pricefx platform.
For a higher level API, see the `pyfx2.api.domain` package.

"""

import csv
import io
import json
import logging
import mimetypes
import os
import re
from abc import ABC, abstractmethod
from enum import Enum, unique
from pathlib import Path
from typing import Any, Dict, Generator, IO, Iterator, List, Optional, Tuple

from pyfx2.lowlevel.avro import AvroStream
from pyfx2.lowlevel.session import PfxSession

from requests.exceptions import HTTPError

from requests_toolbelt import MultipartEncoder

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
    def update_status(
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
    def get_fcs(
        self,
        typedid: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Get the attributes of a specific fields collection."""
        pass

    @abstractmethod
    def list_fcs(
        self,
        type_code: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """List all the fields collections of a given type."""
        pass

    @abstractmethod
    def get_object(
        self,
        typedid: str,
    ) -> Dict[str, Any]:
        """Get the attributes of a specific element."""
        pass

    @abstractmethod
    def list_objects(
        self,
        type_code: str,
    ) -> List[Dict[str, Any]]:
        """List all the elements of a given type."""
        pass

    @abstractmethod
    def add_object(
        self,
        type_code: str,
        attributes: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Add an object and returns its actual attributes."""
        pass

    @abstractmethod
    def update_object(
        self,
        type_code: str,
        attributes: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Update an object and returns its actual attributes."""
        pass

    @abstractmethod
    def stream_fcs(self, typedid: str, chunk_size: int = 128) -> Iterator[bytes]:
        """Stream the content of a data source."""
        pass

    @abstractmethod
    def list_attachments(
        self,
        typedid: str,
    ) -> List[Dict[str, Any]]:
        """Get the information about attachments of the model."""
        pass

    @abstractmethod
    def attach_file(
        self,
        typedid: str,
        name: str,
        content: IO,
    ) -> None:
        """Attach a content as a file with a given name to the model."""
        pass

    @abstractmethod
    def pull_file(
        self, owner_typedid: str, attachment_typedid: str, chunk_size: int = 128
    ) -> Iterator[bytes]:
        """Fetch file from the backend."""
        pass

    @abstractmethod
    def create_table(
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
    def update_table(self, typedid: str, data: AvroStream) -> None:
        """Update data of an already existing DMTable or DMDataSource.

        Values of rows with same keys will be updated.
        Rows with new keys will be appended.

        Args:
            typedid: the typed id of the table to append
            data: the content of the data to push in avro format
        """
        pass

    @abstractmethod
    def get_calcitems(self, typedid: str) -> List[Dict[str, Any]]:
        """Get the calculation items associated with a specific typedid."""
        pass

    @abstractmethod
    def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """Push a new calculation item associated with a specific typedid."""
        pass


# By default, stream download timeout is 60s which may cause errors when the table is large.
# (see https://pricefx.atlassian.net/browse/PFUN-14665)
# When the back end receives a timeout, it chooses an actual timeout which is the minimum value
# between a configured "MAX TIMEOUT" value and the timeout argument.
# The solution here is to set the timeout to 1h so that the back end will always choose this
# configured "MAX TIMEOUT" value up to 1h.
_DATAMART_FETCH_TIMEOUT = 3600


def _to_chunkable_content(
    source: MultipartEncoder, chunk_size: int = 8192
) -> Generator[bytes, None, None]:
    """Returns a generator that prevent request the need of a content lenght.

    In that case, request uses content-encoding: chunked (see
    https://toolbelt.readthedocs.io/en/latest/uploading-data.html#streaming-data-from-a-generator)
    This is needed as we are producing the avro content on the fly and then the final content
    length is unknown.
    TODO maybe actual chuncksize is too small? (see https://github.com/requests/toolbelt/issues/75)
    """
    if not hasattr(source, "read"):
        raise ValueError("given source is not readable")
    buff = source.read(chunk_size)
    while len(buff) > 0:
        yield buff
        buff = source.read(chunk_size)


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

    def update_status(
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

        self.session.post(
            f"{self.endpoint}/optimization.updatejst",
            json={"data": data},
        )

    def get_fcs(self, typedid: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        response = self.session.post(
            f"{self.endpoint}/datamart.getfcs/{typedid}",
            json={"data": params},
        )
        return response.json()["response"]["data"][0]

    def list_fcs(
        self, type_code: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        response = self.session.post(
            f"{self.endpoint}/datamart.getfcs/{type_code}", json={"data": params}
        )
        return response.json()["response"]["data"]

    def get_object(
        self,
        typedid: str,
    ) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        id, type_code = _split_typedid(typedid)
        response = self.session.post(f"{self.endpoint}/fetch/{type_code}/{id}")
        return response.json()["response"]["data"][0]

    def list_objects(
        self,
        type_code: str,
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        response = self.session.post(f"{self.endpoint}/fetch/{type_code}")
        return response.json()["response"]["data"]

    def add_object(self, type_code: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        response = self.session.post(
            f"{self.endpoint}/add/{type_code}",
            json={"data": attributes},
        )
        return response.json()["response"]["data"][0]

    def update_object(self, type_code: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        response = self.session.post(
            f"{self.endpoint}/update/{type_code}",
            json={"data": attributes},
        )
        return response.json()["response"]["data"][0]

    def stream_fcs(self, typedid: str, chunk_size: int = 128) -> Iterator[bytes]:
        """See `Connection` corresponding method."""
        response = self.session.get(
            f"{self.endpoint}/datamart.fetch/{typedid}?stream&timeout={_DATAMART_FETCH_TIMEOUT}",
            params={"output": "csv"},
            stream=True,
        )
        return response.iter_content(chunk_size=chunk_size)

    def list_attachments(
        self,
        typedid: str,
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        response = self.session.post(f"{self.endpoint}/bdmanager.list/{typedid}")
        return response.json()["response"]["data"]

    def attach_file(
        self,
        typedid: str,
        name: str,
        content: IO,
    ) -> None:
        """See `Connection` corresponding method."""
        response = self.session.post(f"{self.endpoint}/uploadmanager.newuploadslot")
        slot_id = response.json()["response"]["data"][0]["id"]

        try:
            attachments = self.session.post(f"{self.endpoint}/bdmanager.list/" + typedid)
            existing = [
                attachment["typedId"].partition(".BD")[0]
                for attachment in attachments.json()["response"]["data"]
                if attachment["fileName"] == name
            ]

            if existing:
                url = f"{self.endpoint}/bdmanager.edit/{typedid}/{existing[0]}/{slot_id}"
            else:
                url = f"{self.endpoint}/bdmanager.upload/{typedid}/{slot_id}"

            self.session.post(
                url,
                files={name: content},
            )
        finally:
            self.session.post(f"{self.endpoint}/uploadmanager.deleteslot/{slot_id}")

    def pull_file(
        self, owner_typedid: str, attachment_typedid: str, chunk_size: int = 128
    ) -> Iterator[bytes]:
        """See `Connection` corresponding method."""
        attachment_id = attachment_typedid.partition(".BD")[0]
        try:
            response = self.session.get(
                f"{self.endpoint}/bdmanager.download/{owner_typedid}/{attachment_id}",
                params={"output": "file"},
                stream=True,
            )
            return response.iter_content(chunk_size=chunk_size)
        except Exception as e:
            raise Exception(f"Error while fetching attached file '{attachment_typedid}'") from e

    def _delete_fc(self, fc_type: str, name: str) -> None:
        self.session.post(
            f"{self.endpoint}/datamart.deletefc/{fc_type}",
            json={"data": {"uniqueName": name}},
        )

    def create_table(
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

        # stream multipart content
        multipart_content = MultipartEncoder(
            fields={
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
        )
        self.session.post_simple(
            f'{self.endpoint}/datamart.createfc/{typecode}{"/replace" if replace_existing else ""}',
            data=_to_chunkable_content(multipart_content),
            headers={"Content-Type": multipart_content.content_type},
        )

    def _fc_spec(self, typedid: str) -> Optional[Dict]:
        objectid = typedid.split(".")[0]
        fc_type = typedid.split(".")[1]
        response = self.session.post(
            f"{self.endpoint}/datamart.getfcs/{fc_type}",
            json={
                "data": {
                    "id": objectid,
                }
            },
        ).json()
        if len(response["response"]["data"]) > 0:
            return response["response"]["data"][0]
        return None

    def update_table(self, typedid: str, data: AvroStream) -> None:
        """See `Connection` corresponding method."""
        fc_entry = self._fc_spec(typedid)
        if fc_entry is None:
            raise RuntimeError(f"Trying to append to non-existing table '{typedid}'")
        response = self.session.post(f"{self.endpoint}/uploadmanager.newuploadslot").json()
        uploadslot = response["response"]["data"][0]["id"]
        try:
            # stream multipart content
            content = MultipartEncoder(
                fields={"DMFieldCollectionData": ("data.avro", data, "avro/binary")}
            )
            self.session.post_simple(
                f"{self.endpoint}/datamart.loadfc/{typedid}",
                data=_to_chunkable_content(content),
                headers={"Content-Type": content.content_type},
            )
        except HTTPError as err:
            error_desc = self.session.post(
                f"{self.endpoint}/uploadmanager.progress/{uploadslot}",
            ).json()["response"]["data"][0]["data"]
            raise Exception(f"Error while uploading file: {error_desc}") from err
        finally:
            self.session.post(f"{self.endpoint}/uploadmanager.deleteslot/{uploadslot}")

    def get_calcitems(self, typedid: str) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        url = f"{self.endpoint}/lookuptablemanager.fetch/{_split_typedid(typedid)[0]}"
        return self.session.post(url).json()["response"]["data"]

    def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """See `Connection` corresponding method."""
        url = f"{self.endpoint}/lookuptablemanager.add/{_split_typedid(typedid)[0]}"
        self.session.post(
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

    def update_status(
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

    def get_fcs(
        self,
        typedid: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """See `Connection` corresponding method.

        This implementation will return a minimal dict.
        """
        return {"typedId": typedid}

    def list_fcs(
        self,
        type_code: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method.

        This implementation of the methods always returns an empty list.
        """
        return []

    def get_object(
        self,
        typedid: str,
    ) -> Dict[str, Any]:
        """See `Connection` corresponding method.

        This implementation will return a minimal dict.
        """
        return {"id": _split_typedid(typedid)[0], "typedId": f"{typedid}"}

    def list_objects(
        self,
        type_code: str,
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method.

        This implementation of the methods always returns an empty list.
        """
        return []

    def add_object(self, type_code: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """See `Connection` corresponding method.

        This implementation of the methods always returns the attributes.
        """
        return attributes

    def update_object(self, type_code: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """See `Connection` corresponding method.

        This implementation of the methods always returns the attributes.
        """
        return attributes

    def stream_fcs(self, typedid: str, chunk_size: int = 128) -> Iterator[bytes]:
        """See `Connection` corresponding method."""
        filepath = self._data_sources_path / f"{typedid}.csv"
        current_pos = 0

        def _read_part() -> bytes:
            nonlocal current_pos
            with open(filepath, "rb") as fin:
                fin.seek(current_pos)
                res = fin.read(chunk_size)
            current_pos += chunk_size
            return res

        return iter(_read_part, b"")

    def list_attachments(
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

    def attach_file(
        self,
        typedid: str,
        name: str,
        content: IO,
    ) -> None:
        """See `Connection` corresponding method."""
        filepath = self._attachments_path(typedid) / name
        if isinstance(content, io.TextIOBase):
            with open(filepath, "w") as textfile:
                textfile.write(content.read())
        else:
            with open(filepath, "wb") as binfile:
                binfile.write(content.read())

    def pull_file(
        self, owner_typedid: str, attachment_typedid: str, chunk_size: int = 128
    ) -> Iterator[bytes]:
        """See `Connection` corresponding method."""
        filepath = self._attachments_path(owner_typedid) / attachment_typedid
        current_pos = 0

        def _read_part() -> bytes:
            nonlocal current_pos
            with open(filepath, "rb") as fin:
                fin.seek(current_pos)
                res = fin.read(chunk_size)
            current_pos += chunk_size
            return res

        return iter(_read_part, b"")

    def create_table(
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

    def update_table(self, typedid: str, data: AvroStream) -> None:
        """See `Connection` corresponding method."""
        filepath = self._data_sources_path / f"{typedid}.avro"
        if not filepath.is_file():
            raise RuntimeError(f"Trying to append to non-existing table '{typedid}'")

        with open(filepath, "ab") as file:
            while buffer := data.read(4096):
                file.write(buffer)

    def get_calcitems(self, typedid: str) -> List[Dict[str, Any]]:
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

    def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """See `Connection` corresponding method."""
        file_path = self.calcitems_path / f"{typedid}.csv"
        if not os.path.exists(file_path):
            with open(file_path, "w") as out:
                out.write("key1,key2,value\n")
        with open(file_path, "a") as out:
            csv.writer(out).writerow([key1, key2, value])

    def _pull_dir(self, dir_type: str, dir_path: str) -> None:
        # Nothing to do
        return

    def _push_result_files(self, result_dir_path: str) -> None:
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

    def update_status(
        self,
        jst_id: int,
        status_code: JobStatus,
        progress: Optional[int],
        msg: Optional[str] = None,
        results: Optional[Dict[str, Any]] = None,
    ) -> None:
        """See `Connection` corresponding method."""
        self._dispatch["job_updates"].update_status(jst_id, status_code, progress, msg, results)

    def get_fcs(
        self,
        typedid: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        return self._default.get_fcs(typedid)

    def list_fcs(
        self,
        type_code: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        return self._default.list_fcs(type_code, params)

    def get_object(
        self,
        typedid: str,
    ) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        return self._default.get_object(typedid)

    def list_objects(
        self,
        type_code: str,
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        return self._default.list_objects(type_code)

    def add_object(self, type_code: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        return self._default.add_object(type_code, attributes)

    def update_object(self, type_code: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """See `Connection` corresponding method."""
        return self._default.update_object(type_code, attributes)

    def stream_fcs(self, typedid: str, chunk_size: int = 128) -> Iterator[bytes]:
        """See `Connection` corresponding method."""
        return self._dispatch["pa_tables"].stream_fcs(typedid, chunk_size)

    def list_attachments(
        self,
        typedid: str,
    ) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        return self._dispatch["model_attachments"].list_attachments(typedid)

    def attach_file(
        self,
        typedid: str,
        name: str,
        content: IO,
    ) -> None:
        """See `Connection` corresponding method."""
        self._dispatch["model_attachments"].attach_file(typedid, name, content)

    def pull_file(
        self, owner_typedid: str, attachment_typedid: str, chunk_size: int = 128
    ) -> Iterator[bytes]:
        """See `Connection` corresponding method."""
        return self._dispatch["model_attachments"].pull_file(
            owner_typedid, attachment_typedid, chunk_size
        )

    def create_table(
        self,
        name: str,
        fields_spec: List[Dict],
        content: AvroStream,
        label: Optional[str] = None,
        owner_typedid: Optional[str] = None,
        replace_existing: bool = True,
    ) -> None:
        """See `Connection` corresponding method."""
        self._dispatch["model_tables"].create_table(
            name, fields_spec, content, label, owner_typedid, replace_existing
        )

    def update_table(self, typedid: str, data: AvroStream) -> None:
        """See `Connection` corresponding method."""
        self._dispatch["model_tables"].update_table(typedid, data)
        pass

    def get_calcitems(self, typedid: str) -> List[Dict[str, Any]]:
        """See `Connection` corresponding method."""
        return self._dispatch["model_parameters"].get_calcitems(typedid)

    def push_calcitem(self, typedid: str, key1: str, key2: str, value: Any) -> None:
        """See `Connection` corresponding method."""
        self._dispatch["model_parameters"].push_calcitem(typedid, key1, key2, value)
