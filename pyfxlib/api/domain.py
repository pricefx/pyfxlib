"""High-level API for pyfx domain objects.

Note that, unless explicitly noted, creating or modifying an object
from this package will *not* update the corresponding platform entity.
"""

from abc import ABC, abstractmethod
import io
from typing import (
    Any,
    Dict,
    Generic,
    IO,
    Iterator,
    List,
    Optional,
    TypeVar,
    Union,
)

import pandas as pd

from pyfxlib.lowlevel import _DEFAULT_STREAM_CHUNK_SIZE, pandasutil, session
from pyfxlib.lowlevel.avro import AvroStream
from pyfxlib.lowlevel.connection import (
    Connection,
    ConnectionRemote,
    ConnectionSync,
    JobStatus,
)
from pyfxlib.lowlevel.session import retry

T = TypeVar("T")


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
        progress: Optional[int] = None,
        msg: Optional[str] = None,
        results: Optional[Dict[str, Any]] = None,
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
        progress: Optional[int],
        msg: Optional[str] = None,
    ) -> None:
        """Update job status progress."""
        self.update_status(progress, msg)

    def set_results(self, results: Dict[str, Any]) -> None:
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
        unique_name: Optional[str] = None,
        label: Optional[str] = None,
        created_by: Optional[int] = None,
        created_date: Optional[str] = None,
        last_update_by: Optional[int] = None,
        last_update_date: Optional[str] = None,
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

    def stream(self, chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE) -> Iterator[bytes]:
        """Stream the content of this table."""
        return self._conn.stream_fcs(self.typedid, chunk_size)

    def to_file(self, file_path: str) -> None:
        """Write table content to file.

        Content will be written in CSV format.
        """
        with open(file_path, "wb") as outfile:
            for buffer in self.stream():
                outfile.write(buffer)

    def to_pandas(self, **args: Dict[str, Any]) -> pd.DataFrame:
        """Get a `pd.DataFrame` with the table content."""
        with io.BytesIO() as buff:
            for data in self.stream():
                buff.write(data)
            buff.seek(0)
            return pd.read_csv(buff, sep=",", **args)


class TableImmutable(AbstractTable):
    """Representation of a data Table."""

    def __init__(
        self,
        conn: ConnectionSync,
        typedid: str,
        unique_name: Optional[str] = None,
        name: Optional[str] = None,
        label: Optional[str] = None,
        created_by: Optional[int] = None,
        created_date: Optional[str] = None,
        last_update_by: Optional[int] = None,
        last_update_date: Optional[str] = None,
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

    @classmethod
    def from_dict(
        cls,
        conn: ConnectionSync,
        attrs: Dict[str, Any],
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
        )


class TableMutable(AbstractTable):
    """Representation of a mutable data Table."""

    def __init__(
        self,
        conn: ConnectionSync,
        typedid: str,
        unique_name: Optional[str] = None,
        name: Optional[str] = None,
        label: Optional[str] = None,
        created_by: Optional[int] = None,
        created_date: Optional[str] = None,
        last_update_by: Optional[int] = None,
        last_update_date: Optional[str] = None,
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

    @classmethod
    def from_dict(
        cls,
        conn: ConnectionSync,
        attrs: Dict[str, Any],
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
        """
        _, conv_dataframe = pandasutil.to_field_collection_spec(
            dataframe, on_unsupported_type=on_unsupported_type, inplace=inplace
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

    def __getitem__(self, i: Union[int, str]) -> Optional[Item]:
        if isinstance(i, int):
            return list(self)[i]
        return self.get_by_name(i)

    def __repr__(self) -> str:
        return str(list(self))

    @abstractmethod
    def get(self, typedid: str) -> Item:
        """Get the item with the given typedid."""
        pass

    def get_by_name(self, name: str) -> Optional[Item]:
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
        req_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._conn = conn
        self.type_code = type_code
        self._params = req_params

    @abstractmethod
    def _table_constructor(self, attrs: Dict[str, Any]) -> TableType:
        pass

    def __iter__(self) -> Iterator[TableType]:
        models_attrs = self._conn.list_fcs(self.type_code, params=self._params)
        return iter([self._table_constructor(attrs) for attrs in models_attrs])

    def get(self, typedid: str) -> TableType:
        """Get the table with the given typedid."""
        return self._table_constructor(self._conn.get_fcs(typedid, params=self._params))


class TableImmutableSource(TableSource[TableImmutable]):
    """Collection of immutable tables."""

    def __init__(
        self,
        conn: ConnectionSync,
        type_code: str,
        req_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        TableSource.__init__(self, conn, type_code, req_params)

    def _table_constructor(self, attrs: Dict[str, Any]) -> TableImmutable:
        return TableImmutable.from_dict(self._conn, attrs)


class TableMutableSource(TableSource[TableMutable]):
    """Collection of mutable tables."""

    def __init__(
        self,
        conn: ConnectionSync,
        type_code: str,
        req_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        TableSource.__init__(self, conn, type_code, req_params)

    def _table_constructor(self, attrs: Dict[str, Any]) -> TableMutable:
        return TableMutable.from_dict(self._conn, attrs)

    def push(
        self,
        name: str,
        fields_spec: List[Dict],
        content: AvroStream,
        label: Optional[str] = None,
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
        """
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
        table_label: Optional[str] = None,
        dimensions: Optional[List[str]] = None,
        on_unsupported_type: str = "error",
        replace_existing: bool = True,
        inplace: bool = False,
        column_labels: Optional[Dict[str, str]] = None,
        manual_fields_specs: Optional[pandasutil.FieldSpecs] = None,
        keep_index: bool = True,
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
        attrs: Dict[str, Any],
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
        value: Optional[str],
        status: Optional[str],
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
        attrs: Dict[str, Any],
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
        label: Optional[str],
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
        attrs: Dict[str, Any],
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
        unique_name: Optional[str] = None,
        label: Optional[str] = None,
        created_by: Optional[int] = None,
        created_date: Optional[str] = None,
        last_update_by: Optional[int] = None,
        last_update_date: Optional[str] = None,
        model_type: Optional[ModelType] = None,
        calcitems_table_id: Optional[str] = None,
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

    def calculation_items(self) -> Optional[CalculationItems]:
        """Get the calculation items of the model."""
        return self._calc_items

    @classmethod
    def from_dict(
        cls,
        conn: ConnectionSync,
        attrs: Dict[str, Any],
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
        return DMModel.from_dict(conn, conn.get_fcs(model_typedid))


class DMModels(ItemCollection[DMModel]):
    """The DataMart Models."""

    def __init__(self, conn: ConnectionSync):
        self._conn = conn

    def __iter__(self) -> Iterator[DMModel]:
        models_attrs = self._conn.list_fcs("DMM")
        return iter([DMModel.from_dict(self._conn, attrs) for attrs in models_attrs])

    def get(self, typedid: str) -> DMModel:
        """Get the DM model with the given typedid."""
        return DMModel.from_dict(self._conn, self._conn.get_fcs(typedid))


class ModelObject(Model):
    """A Model Object."""

    def __init__(
        self,
        conn: ConnectionSync,
        typedid: str,
        unique_name: Optional[str] = None,
        label: Optional[str] = None,
        created_by: Optional[int] = None,
        created_date: Optional[str] = None,
        last_update_by: Optional[int] = None,
        last_update_date: Optional[str] = None,
        model_class: Optional[str] = None,
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
        attrs: Dict[str, Any],
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
        return ModelObject.from_dict(conn, conn.get_object(model_typedid))


class ModelObjects(ItemCollection[ModelObject]):
    """The Models Objects."""

    def __init__(self, conn: ConnectionSync):
        self._conn = conn

    def __iter__(self) -> Iterator[ModelObject]:
        models_attrs = self._conn.list_objects("MO")
        return iter([ModelObject.from_dict(self._conn, attrs) for attrs in models_attrs])

    def get(self, typedid: str) -> ModelObject:
        """Get the model object with the given id."""
        return ModelObject.from_dict(self._conn, self._conn.get_object(typedid))


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


class Instance:
    """A platform instance."""

    def __init__(self, conn: Union[Connection, ConnectionSync]):
        if isinstance(conn, ConnectionSync):
            self._conn = conn
        else:
            self._conn = ConnectionSync(conn)

    def __repr__(self) -> str:
        return f"Instance({self._conn})"

    @classmethod
    def connect(cls, domain: str, partition: str, account: str) -> "Instance":
        """Return an Instance object connecting to a specific instance."""

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

        return Instance(ConnectionRemote(f"https://{domain}/pricefx/{partition}", pfxsession))

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
