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

"""High-level API for pyfx domain objects.

Note that, unless explicitly noted, creating or modifying an object
from this package will *not* update the corresponding platform entity.
"""
from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
import io
from typing import Any, cast, Generic, IO, TypeVar
import warnings

import pandas as pd

from pyfxlib.lowlevel import pandasutil, session
from pyfxlib.lowlevel.avro import AvroStream
from pyfxlib.lowlevel.connection import (
    ConnectionAsync,
    ConnectionRemote,
    ConnectionSync,
)
from pyfxlib.lowlevel.constants import _DEFAULT_PAGE_SIZE, _DEFAULT_STREAM_CHUNK_SIZE
from pyfxlib.lowlevel.session import retry
from pyfxlib.schema.core import JobStatus
from pyfxlib.schema.query import AdvancedCriteria, FieldRule, Operator

T = TypeVar("T")

_EXPOSED_FIELD_METADATA = {"name", "label", "type", "key", "dimension"}


class PlatformJob:
    """A Job executed on the platform."""

    def __init__(self, conn: ConnectionSync, jst_id: int):
        """Get the representation corresponding to a running job.

        Args:
            conn: the underlying connection
            jst_id: the jst id of the Job
        """
        self._conn = conn
        self.jst_id = jst_id

    def update_status(
        self,
        progress: int | None = None,
        msg: str | None = None,
        results: dict[str, Any] | None = None,
    ) -> None:
        """Update the job status on the platform.

        Args:
            progress: the new job progress percent (optional)
            msg: a message for the new status change (optional)
            results: a dictionary associating result names to their value (optional).
                     Will be available to next evaluations and calculations.
        """
        self._conn.update_status(self.jst_id, JobStatus.PROCESSING, progress, msg, results)

    def update_progress(
        self,
        progress: int | None,
        msg: str | None = None,
    ) -> None:
        """Update job status progress."""
        self.update_status(progress, msg)

    def set_results(self, results: dict[str, Any]) -> None:
        """Set the job results."""
        self.update_status(results=results)


class IdentifiableEntity(ABC):
    """An entity with an uniquer identifiant."""

    def __init__(self, conn: ConnectionSync, typedid: str) -> None:
        """Create the repesentation of an identifiable entity.

        Args:
            conn: the underlying connection
            typedid: the typedId of the entity
        """
        self._conn = conn
        self.typedid = typedid


class Owned(ABC):
    """Something owned by an `IdentifiableEntity`."""

    def __init__(self, conn: ConnectionSync, owner: IdentifiableEntity) -> None:
        """Create the repesentation of an Owned entity.

        Args:
            conn: the underlying connection
            owner: the owner
        """
        self._conn = conn
        self._owner = owner

    def owner(self) -> IdentifiableEntity:
        """Get entity owner."""
        return self._owner


class BasicEntity(IdentifiableEntity):
    """Common representation of a lot of entities."""

    def __init__(
        self,
        conn: ConnectionSync,
        typedid: str,
        unique_name: str | None = None,
        label: str | None = None,
        created_by: int | None = None,
        created_date: str | None = None,
        last_update_by: int | None = None,
        last_update_date: str | None = None,
    ):
        """Get the representation corresponding to a pricefx Entity."""
        IdentifiableEntity.__init__(self, conn, typedid)

        self.unique_name = unique_name
        self.label = label
        self.created_by = created_by
        self.created_date = created_date
        self.last_update_by = last_update_by
        self.last_update_date = last_update_date

        # some elements have a distinct "local" name
        # so they will want to override this
        self.name = self.unique_name

    def __repr__(self) -> str:
        if self.unique_name == self.label:
            return f"{self.typedid}({self.label})"
        return f"{self.typedid}({self.label} ({self.name}))"


class AbstractTable(BasicEntity, ABC):
    """Base class for all table types."""

    fields: list[dict[str, Any]]
    """Field specifications of the table, as returned by the Pricefx API.

    Each field is a dict with at least ``name`` (str) and ``type`` (str),
    and optionally ``key`` (bool) and ``dimension`` (bool)...
    """

    def stream(
        self,
        chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE,
        filters: dict[str, Any] | Sequence[FieldRule] | AdvancedCriteria | None = None,
        filter_aggregator: Operator = Operator.AND,
    ) -> Iterator[bytes]:
        """Stream the content of this table.

        Args:
            chunk_size: the size in bytes of the yielded chunks
            filters: optional server-side row filter. Either a `dict` mapping field
                names to values, a sequence of `FieldRule`, or an `AdvancedCriteria`
                for more complex combinations (all combined with `and` by default).
            filter_aggregator: how multiple filters are combined (default: `and`).
                Ignored when `filters` is an `AdvancedCriteria`.
        """
        criteria = (
            None if filters is None else AdvancedCriteria.from_filters(filters, filter_aggregator)
        )
        return self._conn.stream_dm_data(self.typedid, chunk_size, criteria)

    def fetch_paginated(
        self, page_size: int = _DEFAULT_PAGE_SIZE
    ) -> Iterator[list[dict[str, Any]]]:
        """Fetch the content of the table page by page."""
        return self._conn.fetch_paginated_dm_data(self.typedid, page_size)

    def to_file(self, file_path: str) -> None:
        """Write table content to file.

        Content will be written in CSV format.
        """
        with open(file_path, "wb") as outfile:
            for buffer in self.stream():
                outfile.write(buffer)

    def _key_colums(self, df: pd.DataFrame) -> list[str]:
        all_key_cols = [field["name"] for field in self.fields if field.get("key") is True]
        key_cols = [field_name for field_name in all_key_cols if field_name in df.columns]
        if len(key_cols) != len(all_key_cols):
            warnings.warn(
                "The DataFrame doesn't use all the key columns of the source. The index may not be unique."  # noqa: E501 line too long
            )
        return key_cols

    def to_pandas(
        self,
        filters: dict[str, Any] | Sequence[FieldRule] | AdvancedCriteria | None = None,
        filter_aggregator: Operator = Operator.AND,
        **args: dict[str, Any],
    ) -> pd.DataFrame:
        """Get a `pd.DataFrame` with the table content.

        Key columns are set as the DataFrame index.

        Args:
            filters: optional server-side row filter. Either a `dict` mapping field
                names to values, a sequence of `FieldRule`, or an `AdvancedCriteria`.
            filter_aggregator: how multiple filters are combined (default: `and`).
                Ignored when `filters` is an `AdvancedCriteria`.
            **args: extra arguments forwarded to `pd.read_csv`.
        """
        with io.BytesIO() as buff:
            for data in self.stream(filters=filters, filter_aggregator=filter_aggregator):
                buff.write(data)
            buff.seek(0)
            df = cast(pd.DataFrame, pd.read_csv(buff, sep=",", **args))
        key_cols = self._key_colums(df)
        return df.set_index(key_cols) if key_cols else df

    def to_pandas_paginated(self, page_size: int = _DEFAULT_PAGE_SIZE) -> pd.DataFrame:
        """Get a DataFrame via paginated fetch.

        Key columns are set as the DataFrame index
        """
        dfs = []
        for page in self.fetch_paginated(page_size):
            dfs.append(pd.DataFrame(page))
        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        key_cols = self._key_colums(df)
        return df.set_index(key_cols) if key_cols else df

    @staticmethod
    def _filter_field_metadata(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {attr: value for attr, value in field.items() if attr in _EXPOSED_FIELD_METADATA}
            for field in fields
        ]


class TableImmutable(AbstractTable):
    """Representation of a data Table."""

    def __init__(
        self,
        conn: ConnectionSync,
        typedid: str,
        unique_name: str | None = None,
        name: str | None = None,
        label: str | None = None,
        created_by: int | None = None,
        created_date: str | None = None,
        last_update_by: int | None = None,
        last_update_date: str | None = None,
        fields: list[dict[str, Any]] | None = None,
    ) -> None:
        """Get the representation corresponding to a Model Type."""
        BasicEntity.__init__(
            self,
            conn,
            typedid,
            unique_name,
            label,
            created_by,
            created_date,
            last_update_by,
            last_update_date,
        )
        if name is not None:
            self.name = name
        self.fields = self._filter_field_metadata(fields) if fields is not None else []

    @classmethod
    def from_dict(
        cls,
        conn: ConnectionSync,
        attrs: dict[str, Any],
    ) -> "TableImmutable":
        """Create a table from a dict of properties."""
        return TableImmutable(
            conn,
            attrs["typedId"],
            attrs["uniqueName"] if "uniqueName" in attrs else None,
            attrs["name"] if "name" in attrs else None,
            attrs["label"] if "label" in attrs else None,
            attrs["createdBy"] if "createdBy" in attrs else None,
            attrs["createDate"] if "createDate" in attrs else None,
            attrs["lastUpdateBy"] if "lastUpdateBy" in attrs else None,
            attrs["lastUpdateDate"] if "lastUpdateDate" in attrs else None,
            attrs["fields"] if "fields" in attrs else None,
        )


class TableMutable(AbstractTable):
    """Representation of a mutable data Table."""

    def __init__(
        self,
        conn: ConnectionSync,
        typedid: str,
        unique_name: str | None = None,
        name: str | None = None,
        label: str | None = None,
        created_by: int | None = None,
        created_date: str | None = None,
        last_update_by: int | None = None,
        last_update_date: str | None = None,
        fields: list[dict[str, Any]] | None = None,
    ) -> None:
        """Get the representation corresponding to a Model Type."""
        BasicEntity.__init__(
            self,
            conn,
            typedid,
            unique_name,
            label,
            created_by,
            created_date,
            last_update_by,
            last_update_date,
        )
        if name is not None:
            self.name = name
        self.fields = self._filter_field_metadata(fields) if fields is not None else []

    @classmethod
    def from_dict(
        cls,
        conn: ConnectionSync,
        attrs: dict[str, Any],
    ) -> "TableMutable":
        """Create a table from a dict of properties."""
        return TableMutable(
            conn,
            attrs["typedId"],
            attrs["uniqueName"] if "uniqueName" in attrs else None,
            attrs["name"] if "name" in attrs else None,
            attrs["label"] if "label" in attrs else None,
            attrs["createdBy"] if "createdBy" in attrs else None,
            attrs["createDate"] if "createDate" in attrs else None,
            attrs["lastUpdateBy"] if "lastUpdateBy" in attrs else None,
            attrs["lastUpdateDate"] if "lastUpdateDate" in attrs else None,
            attrs["fields"] if "fields" in attrs else None,
        )

    def update(self, data: AvroStream) -> None:
        """Update data the table.

        Values of rows with same keys will be updated.
        Rows with new keys will be appended.

        Args:
            data: content to update in avro format.
        """
        self._conn.update_table(self.typedid, data)

    def update_pandas(
        self,
        dataframe: pd.DataFrame,
        on_unsupported_type: str = "error",
        inplace: bool = False,
        check_oversized_values: bool = False,
    ) -> None:
        """Update this table from a dataframe.

        Values of rows with same index will be updated.
        Rows with new index will be appended.

        Args:
            dataframe: the dataframe to push

            on_unsupported_type: define the behavior when encontering a dataframe column
                                 containing an incompatible type (optional, default: "error"):
            - "error" (default value) raises an error
            - "drop" will drop the column
            - "coerce" will try to convert this column to strings

            inplace: do the required data prep operations in place
                     (will mutate the source dataframe ; optional, default: False)

            check_oversized_values: if True, scan TEXT/LOB columns for values longer than the
                backend accepts and warn about the ones that would be silently truncated on
                push (optional, default: False).
        """
        _, conv_dataframe = pandasutil.to_field_collection_spec(
            dataframe, on_unsupported_type=on_unsupported_type, inplace=inplace
        )
        text_fields_spec = [
            {"name": field.get("name"), "type": field.get("type")}
            for field in self.fields
            if field.get("name") in conv_dataframe.columns and field.get("type") in ["TEXT", "LOB"]
        ]
        pandasutil.advise_about_truncation_risk(
            text_fields_spec, conv_dataframe, check_oversized_values
        )
        retry(lambda: self.update(AvroStream.from_dataframe(conv_dataframe)), 0)


Item = TypeVar("Item", bound=BasicEntity)


class ItemCollection(ABC, Generic[Item]):
    """Common interface for all item collections.

    Items can either be retrieved:

    - as from a list, getting an item by its index, e.g. collection[0],
    - as from a dict, getting an item by its name, e.g. collection["somename"].

    The second method is equivalent to calling `get_by_name` on the collection.
    """

    @abstractmethod
    def __iter__(self) -> Iterator[Item]:
        pass

    def __getitem__(self, i: int | str) -> Item | None:
        if isinstance(i, int):
            return list(self)[i]
        return self.get_by_name(i)

    def __repr__(self) -> str:
        return str(list(self))

    @abstractmethod
    def get(self, typedid: str) -> Item:
        """Get the item with the given typedid."""
        pass

    def get_by_name(self, name: str) -> Item | None:
        """Get an item by name.

        Returns:
          the item with the given name, or None if no such item exists.
        """
        # Note: names are uniques, so candidates is at most of the length 1
        candidates = [item for item in self if item.name == name]
        return candidates[0] if len(candidates) != 0 else None


TableType = TypeVar("TableType", bound=AbstractTable)


class TableSource(ItemCollection[TableType], ABC):
    """Base class for all collections of tables."""

    def __init__(
        self,
        conn: ConnectionSync,
        type_code: str,
        req_params: dict[str, Any] | None = None,
    ) -> None:
        self._conn = conn
        self.type_code = type_code
        self._params = req_params

    @abstractmethod
    def _table_constructor(self, attrs: dict[str, Any]) -> TableType:
        pass

    def __iter__(self) -> Iterator[TableType]:
        models_attrs = self._conn.list_fcs(self.type_code, params=self._params)
        return iter([self._table_constructor(attrs) for attrs in models_attrs])

    def get(self, typedid: str) -> TableType:
        """Get the table with the given typedid."""
        return self._table_constructor(self._conn.get_fc(typedid, params=self._params))


class TableImmutableSource(TableSource[TableImmutable]):
    """Collection of immutable tables."""

    def __init__(
        self,
        conn: ConnectionSync,
        type_code: str,
        req_params: dict[str, Any] | None = None,
    ) -> None:
        TableSource.__init__(self, conn, type_code, req_params)

    def _table_constructor(self, attrs: dict[str, Any]) -> TableImmutable:
        return TableImmutable.from_dict(self._conn, attrs)


class TableMutableSource(TableSource[TableMutable]):
    """Collection of mutable tables."""

    def __init__(
        self,
        conn: ConnectionSync,
        type_code: str,
        req_params: dict[str, Any] | None = None,
    ) -> None:
        TableSource.__init__(self, conn, type_code, req_params)

    def _table_constructor(self, attrs: dict[str, Any]) -> TableMutable:
        return TableMutable.from_dict(self._conn, attrs)

    def push(
        self,
        name: str,
        fields_spec: list[dict],
        content: AvroStream,
        label: str | None = None,
        replace_existing: bool = True,
    ) -> None:
        """Add a new table to the collection of tables on the platform.

        Args:
            name: the name of the new table
            fields_spec: a list of dictionaries that describe the columns of the table
            ```json
            [
               { name:"productId", label:"Product ID", type:"TEXT", key:true},
               { name:"productGroup", label:"ProductGroup", type:"TEXT", dimension:true},
               { name:"revenue", label:"Revenue", type:"MONEY"},
            ]
            ```
            content: the content of the new table
            label: the label of the new table (optional, defaults to name)
            replace_existing: if True then removes the preceding table having the same name if
                              it exists before pushing the data (default = True).

        Raises:
            RuntimeError: if a declared field type requires a newer backend than the one
                connected (e.g. a LOB field against a core older than 16.3.13 / 17.0.4).
        """
        # TODO: temporary LOB version gate — remove this call as part of deleting the whole
        #       gating block once we no longer support backends under 18. See
        #       pandasutil._FIELD_TYPE_MIN_BACKEND_VERSION.
        pandasutil.check_backend_supports_field_types(fields_spec, self._conn.backend_version)
        if isinstance(self, Owned):
            self._conn.create_table(
                name,
                fields_spec,
                content,
                label,
                self.owner().typedid,
                replace_existing,
            )
        else:
            self._conn.create_table(
                name, fields_spec, content, label, replace_existing=replace_existing
            )

    def push_pandas(
        self,
        table_name: str,
        dataframe: pd.DataFrame,
        table_label: str | None = None,
        dimensions: list[str] | None = None,
        on_unsupported_type: str = "error",
        replace_existing: bool = True,
        inplace: bool = False,
        column_labels: dict[str, str] | None = None,
        manual_fields_specs: pandasutil.FieldSpecs | None = None,
        keep_index: bool = True,
        check_oversized_values: bool = False,
    ) -> None:
        """Push a pandas dataframe as a new table.

        Dataframe index will be used as key columns by default.
        Additional key columns can be set manually.

        Args:
            table_name: the name to use for the new table
            dataframe: the dataframe to push
            table_label: the label for the new table (optional, defaults to name)
            dimensions: columns that should be used as dimension (optional, default: None)
            on_unsupported_type: define the behavior when encontering a dataframe column
                containing an incompatible type (optional, default: "error"):
            - "error" (default value) raises an error
            - "drop" will drop the column
            - "coerce" will try to convert this column to strings

            replace_existing: if True then removes the preceding table having the same name if
                it exists before pushing the data (optional, default = True).
            inplace: do the required data prep operations in place
                (will mutate the source dataframe ; optional, default: False)
            column_labels: a dictionary associating dataframe column (including index) name
                to its desired label (optional, default: None).
            manual_fields_specs: manually created specification for exported table fields of
                type FieldSpecs (optional, default: None)
            keep_index: if False, index won't be kept as one of the columns in exported table,
                in this case, at least one of the columns must be set as key in
                manual specs (optional, default: True)
            check_oversized_values: if True, scan TEXT/LOB columns for values longer than the
                backend accepts and warn about the ones that would be silently truncated on
                push (optional, default: False).

        Raises:
            ValueError: if manual_fields_specs is used at the same time
                as column_labels or dimensions
            ValueError: if keep_index is False, but no key field is specified manually
        """
        if manual_fields_specs and (column_labels or dimensions):
            raise ValueError(
                "manual_fields_specs cannot be used to define"
                " column properties at the same time as column_labels or dimensions"
            )
        if (not keep_index) and (manual_fields_specs is None):
            raise ValueError(
                "keep_index can be set to false, only if key field is specified manually"
            )

        fields_spec, conv_dataframe = pandasutil.to_field_collection_spec(
            dataframe,
            dimensions,
            on_unsupported_type,
            inplace,
            column_labels,
        )

        if manual_fields_specs is not None:
            fields_spec, conv_dataframe = pandasutil._update_fields_spec(
                conv_dataframe,
                fields_spec,
                manual_fields_specs.schema,
                keep_index,
                inplace,
            )

        pandasutil.advise_about_truncation_risk(fields_spec, conv_dataframe, check_oversized_values)

        retry(
            lambda: self.push(
                table_name,
                fields_spec,
                AvroStream.from_dataframe(conv_dataframe),
                table_label,
                replace_existing,
            ),
            0,
        )


class Attachment(BasicEntity, Owned):
    """Representation of an Attachment."""

    def __init__(
        self,
        conn: ConnectionSync,
        owner: IdentifiableEntity,
        typedid: str,
        name: str,
        created_by: int,
        created_date: str,
        last_update_by: int,
        last_update_date: str,
        content_type: str,
        length: int,
    ):
        """Create the representation of an Attachment."""
        BasicEntity.__init__(
            self,
            conn,
            typedid,
            name,
            name,
            created_by,
            created_date,
            last_update_by,
            last_update_date,
        )
        Owned.__init__(self, conn, owner)
        self.name = name
        self.content_type = content_type
        self.length = length

    @classmethod
    def from_dict(
        cls,
        conn: ConnectionSync,
        owner: IdentifiableEntity,
        attrs: dict[str, Any],
    ) -> "Attachment":
        """Create an attachment from a dict of properties."""
        return Attachment(
            conn,
            owner,
            attrs["typedId"],
            attrs["fileName"],
            attrs["createdBy"],
            attrs["createDate"],
            attrs["lastUpdateBy"],
            attrs["lastUpdateDate"],
            attrs["contentType"],
            attrs["length"],
        )

    def download_file(self, chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE) -> Iterator[bytes]:
        """Download an attachment.

        Args:
            chunk_size: size of chunks in each iteration (Default: 128)
        """
        return self._conn.pull_file(self.owner().typedid, self.typedid, chunk_size)


class Attachments(Owned, ItemCollection[Attachment]):
    """Attachments of an entity."""

    def push(
        self,
        name: str,
        content: IO,
    ) -> None:
        """Attach new content to owner on the platform.

        Args:
            name: name of the attachment
            content: content of the attachment
        """
        self._conn.attach_file(self.owner().typedid, name, content)

    def __iter__(self) -> Iterator[Attachment]:
        attach_attrs = self._conn.list_attachments(self.owner().typedid)
        return iter(
            [Attachment.from_dict(self._conn, self.owner(), attrs) for attrs in attach_attrs]
        )

    def get(self, typedid: str) -> Attachment:
        """Get the attachment with the given typedid."""
        candidates = [attach for attach in self if attach.typedid == typedid]
        if len(candidates) == 0:
            raise RuntimeError(f"No attachment with typedid {typedid}.")
        return candidates[0]


class ModelTables(TableMutableSource, Owned):
    """Tables owned by a model."""

    def __init__(self, conn: ConnectionSync, owner: IdentifiableEntity):
        TableMutableSource.__init__(self, conn, "DMT", {"owner": str(owner.typedid)})
        Owned.__init__(self, conn, owner)


class CalculationItem(IdentifiableEntity, Owned):
    """Representation of a CalculationItem."""

    def __init__(
        self,
        conn: ConnectionSync,
        owner: IdentifiableEntity,
        typedid: str,
        key1: str,
        key2: str,
        value: str | None,
        status: str | None,
        created_by: int,
        created_date: str,
        last_update_by: int,
        last_update_date: str,
    ):
        """Create the representation of a Calculation Item."""
        IdentifiableEntity.__init__(self, conn, typedid)
        Owned.__init__(self, conn, owner)
        self.key1 = key1
        self.key2 = key2
        self.value = value
        self.status = status
        self.created_by = created_by
        self.created_date = created_date
        self.last_update_by = last_update_by
        self.last_update_date = last_update_date

    def __repr__(self) -> str:
        return f"CalcItem[{self.status}]({self.key1}, {self.key2}, {self.value})"

    @classmethod
    def from_dict(
        cls,
        conn: ConnectionSync,
        owner: IdentifiableEntity,
        attrs: dict[str, Any],
    ) -> "CalculationItem":
        """Create an attachment from a dict of properties."""
        value = None
        if "attributeExtension___Value" in attrs:
            value = attrs["attributeExtension___Value"]
        status = None
        if "attributeExtension___Status" in attrs:
            status = attrs["attributeExtension___Status"]
        return CalculationItem(
            conn,
            owner,
            attrs["typedId"],
            attrs["key1"],
            attrs["key1"],
            value,
            status,
            attrs["createdBy"],
            attrs["createDate"],
            attrs["lastUpdateBy"],
            attrs["lastUpdateDate"],
        )


class CalculationItems(IdentifiableEntity, Owned):
    """Calculations items of an entity."""

    def __init__(
        self,
        conn: ConnectionSync,
        owner: IdentifiableEntity,
        typedid: str,
    ):
        IdentifiableEntity.__init__(self, conn, typedid)
        Owned.__init__(self, conn, owner)

    def __iter__(self) -> Iterator[CalculationItem]:
        items = self._conn.get_calcitems(self.typedid)
        return iter([CalculationItem.from_dict(self._conn, self, item) for item in items])

    def __getitem__(self, i: int) -> CalculationItem:
        return list(self)[i]

    def __repr__(self) -> str:
        return str(list(self))

    def push(
        self,
        key1: str,
        key2: str,
        value: Any,
    ) -> None:
        """Add a new calculation item."""
        self._conn.push_calcitem(self.typedid, key1, key2, value)


class ModelType(BasicEntity):
    """A Model Type."""

    def __init__(
        self,
        conn: ConnectionSync,
        typedid: str,
        unique_name: str,
        label: str | None,
        created_by: int,
        created_date: str,
        last_update_by: int,
        last_update_date: str,
        nature: str,
        definition: str,
    ) -> None:
        """Get the representation corresponding to a Model Type."""
        BasicEntity.__init__(
            self,
            conn,
            typedid,
            unique_name,
            label,
            created_by,
            created_date,
            last_update_by,
            last_update_date,
        )

        self.nature = nature
        self.definition = definition

    @classmethod
    def from_dict(
        cls,
        conn: ConnectionSync,
        attrs: dict[str, Any],
    ) -> "ModelType":
        """Create a model type from a dict of properties."""
        return ModelType(
            conn,
            attrs["typedId"],
            attrs["uniqueName"],
            attrs["label"] if "label" in attrs else None,
            attrs["createdBy"],
            attrs["createDate"],
            attrs["lastUpdateBy"],
            attrs["lastUpdateDate"],
            attrs["modelNatureUN"],
            attrs["modelTypeDefJson"],
        )


class Model(BasicEntity, ABC):
    """Model abstract interface."""

    @abstractmethod
    def attachments(self) -> Attachments:
        """Get the model attachments."""
        pass

    @abstractmethod
    def tables(self) -> ModelTables:
        """Get the tables owned by the model."""
        pass


class DMModel(Model):
    """A DM model."""

    def __init__(
        self,
        conn: ConnectionSync,
        typedid: str,
        unique_name: str | None = None,
        label: str | None = None,
        created_by: int | None = None,
        created_date: str | None = None,
        last_update_by: int | None = None,
        last_update_date: str | None = None,
        model_type: ModelType | None = None,
        calcitems_table_id: str | None = None,
    ):
        """Get the representation corresponding to a DM Model."""
        Model.__init__(
            self,
            conn,
            typedid,
            unique_name,
            label,
            created_by,
            created_date,
            last_update_by,
            last_update_date,
        )

        self.model_type = model_type

        self._attachments = Attachments(conn, self)
        self._tables = ModelTables(conn, self)

        if calcitems_table_id is None:
            self._calc_items = None
        else:
            self._calc_items = CalculationItems(conn, self, calcitems_table_id)

    def attachments(self) -> Attachments:
        """Get the model attachments."""
        return self._attachments

    def tables(self) -> ModelTables:
        """Get the tables owned by the model."""
        return self._tables

    def calculation_items(self) -> CalculationItems | None:
        """Get the calculation items of the model."""
        return self._calc_items

    @classmethod
    def from_dict(
        cls,
        conn: ConnectionSync,
        attrs: dict[str, Any],
    ) -> "DMModel":
        """Create a model from a dict of properties."""
        model_type = None
        if "modelType" in attrs and attrs["modelType"] is not None:
            model_type = ModelType.from_dict(conn, attrs["modelType"])

        calc_items_id = None
        if "calcItemsLookupTableId" in attrs and attrs["calcItemsLookupTableId"] is not None:
            calc_items_id = attrs["calcItemsLookupTableId"]

        return DMModel(
            conn,
            attrs["typedId"],
            attrs["uniqueName"] if "uniqueName" in attrs else None,
            attrs["label"] if "label" in attrs else None,
            attrs["createdBy"] if "createdBy" in attrs else None,
            attrs["createDate"] if "createDate" in attrs else None,
            attrs["lastUpdateBy"] if "lastUpdateBy" in attrs else None,
            attrs["lastUpdateDate"] if "lastUpdateDate" in attrs else None,
            model_type,
            calc_items_id,
        )

    @classmethod
    def from_conn(
        cls,
        conn: ConnectionSync,
        model_typedid: str,
    ) -> "DMModel":
        """Create a model from a model typedid."""
        return DMModel.from_dict(conn, conn.get_fc(model_typedid))


class DMModels(ItemCollection[DMModel]):
    """The DataMart Models."""

    def __init__(self, conn: ConnectionSync):
        self._conn = conn

    def __iter__(self) -> Iterator[DMModel]:
        models_attrs = self._conn.list_fcs("DMM")
        return iter([DMModel.from_dict(self._conn, attrs) for attrs in models_attrs])

    def get(self, typedid: str) -> DMModel:
        """Get the DM model with the given typedid."""
        return DMModel.from_dict(self._conn, self._conn.get_fc(typedid))


class ModelObject(Model):
    """A Model Object."""

    def __init__(
        self,
        conn: ConnectionSync,
        typedid: str,
        unique_name: str | None = None,
        label: str | None = None,
        created_by: int | None = None,
        created_date: str | None = None,
        last_update_by: int | None = None,
        last_update_date: str | None = None,
        model_class: str | None = None,
    ):
        """Get the representation corresponding to a ModelObject."""
        Model.__init__(
            self,
            conn,
            typedid,
            unique_name,
            label,
            created_by,
            created_date,
            last_update_by,
            last_update_date,
        )

        self.model_class = model_class

        self._attachments = Attachments(conn, self)
        self._tables = ModelTables(conn, self)

    def attachments(self) -> Attachments:
        """Get the model attachments."""
        return self._attachments

    def tables(self) -> ModelTables:
        """Get the tables owned by the model."""
        return self._tables

    @classmethod
    def from_dict(
        cls,
        conn: ConnectionSync,
        attrs: dict[str, Any],
    ) -> "ModelObject":
        """Create a model from a dict of properties."""
        return ModelObject(
            conn,
            attrs["typedId"],
            attrs["uniqueName"] if "uniqueName" in attrs else None,
            attrs["label"] if "label" in attrs else None,
            attrs["createdBy"] if "createdBy" in attrs else None,
            attrs["createDate"] if "createDate" in attrs else None,
            attrs["lastUpdateBy"] if "lastUpdateBy" in attrs else None,
            attrs["lastUpdateDate"] if "lastUpdateDate" in attrs else None,
            attrs["modelClass"] if "modelClass" in attrs else None,
        )

    @classmethod
    def from_conn(
        cls,
        conn: ConnectionSync,
        model_typedid: str,
    ) -> "ModelObject":
        """Create a model from a model typedid."""
        data = conn.get_object(model_typedid)
        if data is None:
            raise ValueError(f"Object not found: {model_typedid}")
        return ModelObject.from_dict(conn, data)


class ModelObjects(ItemCollection[ModelObject]):
    """The Models Objects."""

    def __init__(self, conn: ConnectionSync):
        self._conn = conn

    def __iter__(self) -> Iterator[ModelObject]:
        models_attrs = self._conn.list_objects("MO")
        return iter([ModelObject.from_dict(self._conn, attrs) for attrs in models_attrs])

    def get(self, typedid: str) -> ModelObject:
        """Get the model object with the given id."""
        data = self._conn.get_object(typedid)
        if data is None:
            raise ValueError(f"Object not found: {typedid}")
        return ModelObject.from_dict(self._conn, data)


class DataSources(TableMutableSource):
    """The data sources."""

    def __init__(self, conn: ConnectionSync):
        TableMutableSource.__init__(self, conn, "DMDS")
        self._conn = conn


class Datamarts(TableImmutableSource):
    """The datamarts."""

    def __init__(self, conn: ConnectionSync):
        TableImmutableSource.__init__(self, conn, "DM")
        self._conn = conn


class Partition:
    """A platform partition."""

    def __init__(self, conn: ConnectionAsync | ConnectionSync):
        if isinstance(conn, ConnectionSync):
            self._conn = conn
        else:
            self._conn = ConnectionSync(conn)

    def __repr__(self) -> str:
        return f"Partition({self._conn})"

    @classmethod
    def connect(cls, domain: str, partition: str, account: str) -> "Partition":
        """Return a Partition object connecting to a specific instance."""

        def _try_storage_then_prompt_for_pass() -> str:
            try:
                return session.PasswordProviderLinuxSecretStore(domain, partition, account)()
            except RuntimeError:
                return session.PasswordProviderPrompt(
                    f"password for {account}@{domain}/{partition}: "
                )()

        pfxsession = session.pfx_session_from_user_pass(
            domain,
            partition,
            account,
            _try_storage_then_prompt_for_pass,
        )

        return Partition(ConnectionRemote(f"https://{domain}/pricefx/{partition}", pfxsession))

    def models(self) -> DMModels:
        """Get the models stored on the instance."""
        return DMModels(self._conn)

    def model_objects(self) -> ModelObjects:
        """Get the model objects stored on the instance."""
        return ModelObjects(self._conn)

    def datasources(self) -> DataSources:
        """Get the data sources stored on the instance."""
        return DataSources(self._conn)

    def datamarts(self) -> Datamarts:
        """Get the data sources stored on the instance."""
        return Datamarts(self._conn)


# Deprecated class, use Partition instead. Kept for backward compatibility only.
Instance = Partition
