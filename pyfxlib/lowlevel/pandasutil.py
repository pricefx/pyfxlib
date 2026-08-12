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

"""Utility functions to convert pandas DataFrames to avro and pricefx FieldCollections.

Contains also schema type conversion functions
- from pandas to avro
- from pandas to pricefx FieldCollections.
"""

from collections.abc import Callable
import datetime
from enum import StrEnum
import logging
from typing import Any, NamedTuple, Optional

import numpy as np
import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_integer_dtype,
    is_numeric_dtype,
    is_string_dtype,
)

from pyfxlib.schema import BackendVersion

LOGGER = logging.getLogger(__name__)


def to_pricefx_type(column: pd.Series) -> Optional[str]:
    """Returns the pricefx column type corresponding to the given pandas Series data.

    Return None if the given numpy type is not supported.

    Args:
        column: the column data as pd.Series
    """
    col_type = _column_type(column)
    return col_type.pricefx if col_type is not None else None


def to_avro_type(column: pd.Series) -> Optional[str | dict[str, Any]]:
    """Returns the avro type declaration corresponding to the given pandas Series data.

    Return None if the given numpy type is not supported.

    Args:
        column: the column data as pd.Series
    """
    col_type = _column_type(column)
    return col_type.avro if col_type is not None else None


def to_field_collection_spec(
    dataframe: pd.DataFrame,
    dimensions: Optional[list[str]] = None,
    on_unsupported_type: str = "error",
    inplace: bool = False,
    column_labels: Optional[dict[str, str]] = None,
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    """
    Returns the pricefx field collection spec and the corresponding DataFrame.

    Args:
        dataframe: the dataframe to push

        dimensions: columns that should be used as dimension (optional, default: None)

        on_unsupported_type: define the behavior when encontering a dataframe column
                             containing an incompatible type (optional, default: "error"):
        - "error" (default value) raises an error
        - "drop" will drop the column
        - "coerce" will try to convert this column to strings

        inplace: do the required data prep operations in place
                 (will mutate the source dataframe ; optional, default: False)

        column_labels: a dictionary associating dataframe column (including index) name to
                       its desired label (optional, default: None).
    """
    assert on_unsupported_type in ["coerce", "drop", "error"]
    # get key(s) from index
    if dataframe.index.nlevels == 1:
        index_name = dataframe.index.names[0]
        if index_name is None:
            index_name = "index"  # default col value for index with no name
        keys = [index_name]
    else:
        keys = []
        for level, index_name in enumerate(dataframe.index.names):
            if index_name is None:
                index_name = f"level_{level}"  # default col value for index with no name
            keys.append(index_name)

    # make index part of the columns
    if inplace:
        dataframe.reset_index(inplace=True)
    else:
        dataframe = dataframe.reset_index()

    labels = column_labels if column_labels is not None else {}
    unknows_col_labels = [col for col in labels.keys() if col not in dataframe.columns]
    if unknows_col_labels:
        raise ValueError(
            f"column_labels contains at least one unknown column name: {unknows_col_labels}"
        )

    # create field spec and convert/drop_columns
    fields_spec = []
    unsupported_types_errors = []
    for col in dataframe.columns:
        pfx_type = to_pricefx_type(dataframe[col])
        if pfx_type is None:
            if on_unsupported_type == "coerce":
                dataframe[col] = dataframe[col].astype(str)
                pfx_type = "TEXT"
            if on_unsupported_type == "drop":
                continue
            if on_unsupported_type == "error":
                unsupported_types_errors.append(f"('{col}': '{_column_dtype(dataframe[col])}')")
                continue

        fields_spec.append(
            {
                "name": col,
                "label": col if col not in labels else labels[col],
                "type": pfx_type,
                "key": col in keys,
                "dimension": dimensions is not None and col in dimensions,
            }
        )

    if len(unsupported_types_errors) > 0:
        raise ValueError("unsupported types for columns: " + ", ".join(unsupported_types_errors))

    return (fields_spec, dataframe.loc[:, [spec["name"] for spec in fields_spec]])


class FieldSpecs:
    """Structure for keeping user specifications for exported table fields."""

    class MeasureTypes(StrEnum):
        """Enum for keeping allowed measure types."""

        EXTENDED = "EXTENDED"
        FIXED = "FIXED"
        PERUNIT = "PERUNIT"

    class _DtypeAndCheck(NamedTuple):
        retype_func: Callable[[pd.DataFrame, str], pd.Series]
        check: Callable[[pd.Series], bool]

    _field_type_to_dtype = {
        "INTEGER": _DtypeAndCheck(
            lambda df, col_name: df[col_name].astype("int"), lambda col: is_integer_dtype(col)
        ),
        "NUMBER": _DtypeAndCheck(
            lambda df, col_name: df[col_name].astype("double"), lambda col: is_numeric_dtype(col)
        ),
        "TEXT": _DtypeAndCheck(
            lambda df, col_name: df[col_name].astype("string"), lambda col: is_string_dtype(col)
        ),
        "DATE": _DtypeAndCheck(
            lambda df, col_name: pd.to_datetime(df[col_name]).dt.date,
            lambda col: isinstance(col.dtype, type) and issubclass(col.dtype, datetime.date),
        ),
        "DATETIME": _DtypeAndCheck(
            lambda df, col_name: df[col_name].astype("datetime64[ns]"),
            lambda col: is_datetime64_any_dtype(col),
        ),
        "BOOLEAN": _DtypeAndCheck(
            lambda df, col_name: df[col_name].astype("bool"), lambda col: is_bool_dtype(col)
        ),
    }

    for extended_type, base_type in [
        ("CURRENCY", "TEXT"),
        ("MONEY", "NUMBER"),
        ("QUANTITY", "NUMBER"),
        ("UOM", "TEXT"),
        # LOB is a large-text field type. It is stored/encoded exactly like TEXT
        # (an Avro string); it is never auto-inferred and must be set explicitly via
        # manual field specs. The backend maps a STRING Avro payload to a LOB field,
        # but only from the core versions declared in _FIELD_TYPE_MIN_BACKEND_VERSION.
        ("LOB", "TEXT"),
    ]:
        _field_type_to_dtype[extended_type] = _field_type_to_dtype[base_type]

    @property
    def field_type_to_dtype(self) -> dict[str, _DtypeAndCheck]:
        """Returns dict with check and retype for each FieldType."""
        return self._field_type_to_dtype

    def __init__(self) -> None:
        self.schema: dict[str, dict[str, Any]] = {}

    def set_col_specs(
        self,
        col_name: str,
        name: Optional[str] = None,
        label: Optional[str] = None,
        type: Optional[str] = None,
        key: Optional[bool] = False,
        distribution_key: Optional[bool] = False,
        dimension: Optional[bool] = False,
        format: Optional[str] = None,
        measure_type: Optional[str] = None,
    ) -> None:
        """Set specs for single column.

        Args:
            col_name: current name of the column, present in dataframe
            name: name of the column in exported table, if no value is provided,
                col_name is used (optional, default: None)
            label: column label in the exported table, if no value is provided,
                name is used (optional, default: None)
            type: FieldType of column in exported table, if no value is provided,
                type is assigned automatically (optional, default: None)
            key: boolean flag for key columns (optional, default: False)
            distribution_key: boolean flag for distribution key (optional, default: False)
            dimension: boolean flag for dimension (optional, default: False)
            format: allows to set formatting of the values present in column
                (optional, default: None)
            measure_type: columns measure type (optional, default: None)
        Raises:
            ValueError: If type is set to unknown FieldType
            ValueError: If measure_type is set to unsupported one
        """
        imputed_name = name if name else col_name
        col_schema = {
            "name": imputed_name,
            "label": label if label else imputed_name,
            "key": key,
            "dimension": dimension,
        }

        if distribution_key:
            col_schema["key"] = True
            col_schema["distributionKey"] = True

        if type:
            col_schema["type"] = type
            if type not in self._field_type_to_dtype:
                raise ValueError(
                    f"unsupported FieldType {type} for column {col_name},"
                    f" expected types are {str.join(', ', self._field_type_to_dtype)}"
                )

        if format is not None:
            col_schema["format"] = format

        if measure_type is not None:
            col_schema["measureType"] = measure_type
            if measure_type not in self.MeasureTypes:
                raise ValueError(
                    f"unsupported measure type {measure_type} for column {col_name},"
                    f" expected types are {str.join(', ', self.MeasureTypes)}"
                )

        self.schema[col_name] = col_schema


# ---------------------------------------------------------------------------------------
# TEMPORARY LOB version gating. This whole block (the map below plus _backend_supports_
# field_type, _format_version_requirement, check_backend_supports_field_types, and the call
# in DataSources.push) exists only because LOB push needs the core AvroData gate fix, which
# lands per major (16.3.13 / 17.0.4). It is scaffolding, not a general mechanism.
#
# TODO: remove this entire block once we no longer support backends under 18 (every
#       supported backend then carries the fix). Deleting the map, the three helpers and
#       the push call site restores the plain, ungated push path.
# ---------------------------------------------------------------------------------------
#
# Minimum Pricefx core version that supports a given field type on the push path, declared
# as one lower bound per major version: {major: (major, minor, patch)}. This is the version
# requirement carried by the field type itself; push checks it against the live backend
# version before sending the data.
#
# It is deliberately NOT a single scalar floor: the backend support for a type is
# backported per major, so version numbers are not monotonic across majors with respect to
# "has the fix". For example 17.0.3 is numerically higher than 16.3.13 yet predates the
# 17.0 backport. Within a single major versions ARE monotonic, so one lower bound per major
# is enough. A backend on a major newer than every listed major is assumed to support the
# type (cut from a develop that already had the fix). Types absent from this map have no
# version requirement.
_FIELD_TYPE_MIN_BACKEND_VERSION: dict[str, dict[int, tuple[int, int, int]]] = {
    # LOB push requires the core AvroData gate fix (accept a STRING avro payload for a
    # LOB field), backported to 16.3.13 on the 16.x major and 17.0.4 on the 17.x major.
    "LOB": {16: (16, 3, 13), 17: (17, 0, 4)},
}


def _backend_supports_field_type(field_type: str, backend_version: BackendVersion) -> bool:
    """Whether a backend at the given version supports pushing `field_type`.

    Field types without a declared requirement are always supported.
    """
    bounds = _FIELD_TYPE_MIN_BACKEND_VERSION.get(field_type)
    if not bounds:
        return True
    major = backend_version.major
    version = (major, backend_version.minor or 0, backend_version.patch or 0)
    if major in bounds:
        # Same major as a declared bound: versions are monotonic within a major.
        return version >= bounds[major]
    # No bound for this major: supported only if the major is newer than every declared
    # major (assumed to carry the fix); older majors are unsupported.
    return major > max(bounds)


def _format_version_requirement(field_type: str) -> str:
    """Human-readable "X.Y.Z or A.B.C" requirement string for a gated field type."""
    return " or ".join(
        f"{major}.{minor}.{patch}"
        for major, minor, patch in sorted(_FIELD_TYPE_MIN_BACKEND_VERSION[field_type].values())
    )


def check_backend_supports_field_types(
    fields_spec: list[dict[str, Any]],
    backend_version_provider: Callable[[], BackendVersion],
) -> None:
    """Ensure the backend version supports every version-gated field type being pushed.

    The backend version is fetched lazily through `backend_version_provider`, and only
    when the fields actually contain a version-gated type, so an ordinary push pays no
    extra round-trip.

    Args:
        fields_spec: the field specification list about to be pushed.
        backend_version_provider: callable returning the backend version.
    Raises:
        RuntimeError: if a declared field type is not supported by the backend version.
    """
    gated = [field for field in fields_spec if field.get("type") in _FIELD_TYPE_MIN_BACKEND_VERSION]
    if not gated:
        return
    backend_version = backend_version_provider()
    for field in gated:
        field_type = field["type"]
        if not _backend_supports_field_type(field_type, backend_version):
            raise RuntimeError(
                f"Field {field.get('name', '?')!r} of type {field_type} requires a newer"
                f" Pricefx core: {field_type} is supported from"
                f" {_format_version_requirement(field_type)} up."
                f" Current backend version: {backend_version}"
            )


# Maximum value length the Pricefx core accepts for a given field type before silently
# truncating it (net.pricefx.common.api.pa.DataType#parse truncates rather than rejecting).
# Duplicated here from core's DataType.MAX_STRING_LENGTH/MAX_LOB_LENGTH: pyfxlib has no way
# to fetch these from the backend, so if core ever changes them, this map must be updated too.
_FIELD_TYPE_MAX_VALUE_LENGTH: dict[str, int] = {
    "TEXT": 255,
    "LOB": 10000,
}


def warn_about_oversized_values(
    fields_spec: list[dict[str, Any]], conv_dataframe: pd.DataFrame
) -> None:
    """Warn (does not raise) about values that the backend will silently truncate on push.

    Args:
        fields_spec: the field specification list about to be pushed.
        conv_dataframe: the dataframe about to be pushed, with column names matching
            `fields_spec`.
    """
    for field in fields_spec:
        max_length = _FIELD_TYPE_MAX_VALUE_LENGTH.get(field.get("type", ""))
        col_name = field.get("name")
        if max_length is None or col_name not in conv_dataframe:
            continue
        lengths = conv_dataframe[col_name].dropna().astype(str).str.len()
        overflowing = lengths[lengths > max_length]
        if not overflowing.empty:
            suggestion = (
                " Consider declaring this field as LOB in manual_fields_specs instead."
                if field["type"] == "TEXT"
                else ""
            )
            LOGGER.warning(
                "Field %r of type %s has %d value(s) longer than %d characters (longest: %d)."
                " The Pricefx backend will silently truncate them to %d characters on push.%s",
                col_name,
                field["type"],
                len(overflowing),
                max_length,
                int(lengths.max()),
                max_length,
                suggestion,
            )


_TRUNCATION_RISK_ADVISED = False


def advise_about_truncation_risk(
    fields_spec: list[dict[str, Any]], conv_dataframe: pd.DataFrame, with_check: bool = False
) -> None:
    """Warn once per process that TEXT/LOB values may be silently truncated by the backend.

    Args:
        fields_spec: the field specification list about to be pushed.
        conv_dataframe: dataframe to push, already modified by to_field_collection_spec(),
                used only if with_check is set to True.
        with_check: if True, scan TEXT/LOB columns for values longer than the
                backend accepts and warn about the ones that would be silently truncated on
                push (optional, default: False).
    """
    global _TRUNCATION_RISK_ADVISED
    existing_text_fields = any(
        field.get("type") in _FIELD_TYPE_MAX_VALUE_LENGTH for field in fields_spec
    )
    if not _TRUNCATION_RISK_ADVISED and existing_text_fields:
        _TRUNCATION_RISK_ADVISED = True
        LOGGER.warning(
            "Pushing TEXT or LOB field(s): the Pricefx backend silently truncates values longer"
            " than %d characters for TEXT, %d for LOB, without raising any error. You can pass"
            " check_oversized_values=True to push_pandas/update_pandas to check whether any of"
            " your values are actually affected.",
            _FIELD_TYPE_MAX_VALUE_LENGTH["TEXT"],
            _FIELD_TYPE_MAX_VALUE_LENGTH["LOB"],
        )
    if existing_text_fields and with_check:
        warn_about_oversized_values(fields_spec, conv_dataframe)


def _update_fields_spec(
    conv_dataframe: pd.DataFrame,
    current_fields_spec_list: list[dict[str, Any]],
    updated_fields_spec: dict[str, dict[str, Any]],
    keep_index: bool,
    inplace: bool,
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    """Update default field specifications with manual specifications.

    Args:
        conv_dataframe: dataframe to push, already modified by to_field_collection_spec()
        current_fields_spec_list: fields_spec list generated by to_field_collection_spec()
        updated_fields_spec: manual fields spec, used to update fields_spec
        keep_index: boolean flag marking whether index should be kept,
            and pushed as one of the columns
        inplace: do the required data prep operations in place
            (will mutate the source dataframe)

    Returns: tuple containing updated fields_spec and dataframe
    Raises:
        ValueError: if keep_index is set to false and no column is set as key
        ValueError: if updated fields specification exist for column not present in data
    """
    current_fields_spec = {col_spec["name"]: col_spec for col_spec in current_fields_spec_list}

    if not keep_index:
        index_cols = [
            col_name for col_name, col_spec in current_fields_spec.items() if col_spec["key"]
        ]
        manual_key_cols = [
            col_name
            for col_name in updated_fields_spec
            if (
                updated_fields_spec[col_name]["key"]
                and (col_name in conv_dataframe.columns)
                and (col_name not in index_cols)
            )
        ]

        if manual_key_cols == []:
            raise ValueError("keep_index is set to false, and no valid column is set as a key")

        for col_name in index_cols:
            current_fields_spec.pop(col_name, None)
        if inplace:
            conv_dataframe.drop(columns=index_cols, inplace=True)
        else:
            conv_dataframe = conv_dataframe.drop(columns=index_cols)

    for col_name in updated_fields_spec:
        if col_name not in conv_dataframe:
            raise ValueError(
                f"Updated fields specification present for column {col_name},"
                f" which doesn't exist in dataframe"
            )
        # check if col has right dtype matching desired Field Type, recast otherwise
        if (
            "type" in updated_fields_spec[col_name]
            and updated_fields_spec[col_name]["type"] != current_fields_spec[col_name]["type"]
        ):

            retype_func, check = FieldSpecs().field_type_to_dtype[
                updated_fields_spec[col_name]["type"]
            ]  # type: ignore
            if not check(conv_dataframe[col_name]):
                conv_dataframe[col_name] = retype_func(conv_dataframe, col_name)

        current_fields_spec[col_name].update(updated_fields_spec[col_name])

    rename_cols = {col_name: col_spec["name"] for col_name, col_spec in current_fields_spec.items()}

    if inplace:
        conv_dataframe.rename(columns=rename_cols, inplace=True)
    else:
        conv_dataframe = conv_dataframe.rename(columns=rename_cols)

    return list(current_fields_spec.values()), conv_dataframe


class _ColumnType(NamedTuple):
    avro: str | dict[str, Any]
    pricefx: str


def _column_type(column: pd.Series) -> Optional[_ColumnType]:
    dtype = _column_dtype(column)
    if (isinstance(dtype, type) and issubclass(dtype, str)) or pd.StringDtype().is_dtype(dtype):
        return _ColumnType("string", "TEXT")
    if isinstance(dtype, type) and issubclass(dtype, datetime.date):
        return _ColumnType({"type": "int", "logicalType": "date"}, "DATE")
    if pd.api.types.is_bool_dtype(dtype):
        return _ColumnType("boolean", "BOOLEAN")
    if pd.api.types.is_datetime64_ns_dtype(dtype):
        return _ColumnType({"type": "long", "logicalType": "timestamp-millis"}, "DATETIME")
    if dtype == np.uint64:
        return _ColumnType({"type": "long", "unsigned": True}, "INTEGER")
    if dtype == np.int64:
        return _ColumnType("long", "INTEGER")
    if pd.api.types.is_signed_integer_dtype(dtype):
        return _ColumnType("int", "INTEGER")
    if pd.api.types.is_unsigned_integer_dtype(dtype):
        return _ColumnType({"type": "int", "unsigned": True}, "INTEGER")
    if dtype == np.float32:
        return _ColumnType("float", "NUMBER")
    if dtype == np.float64:
        return _ColumnType("double", "NUMBER")
    if pd.api.types.is_float_dtype(dtype):
        return _ColumnType("double", "NUMBER")
    return None


def _column_dtype(column: pd.Series) -> type | np.dtype:
    """Returns most possible precise column data type.

    If it is an Object Series (dtype('O')),
    - returns the first non na value type
    - returns str if the column contains only na
    Otherwise, returns the column numpy dtype
    """
    if pd.api.types.is_object_dtype(column):
        # Assumes that the column object types is uniform
        # - we don't check the whole column for performances reasons
        # - if there are mixed types in the column then fastavro will
        #   fail.
        # so the type is defined from the first non na value.
        not_empty = column.loc[column.notna()]
        if not_empty.shape[0] != 0:
            return type(not_empty.iloc[0])
        # if nothing is found then default type is str
        return str
    else:
        return column.dtype
