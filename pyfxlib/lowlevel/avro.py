"""Utility functions to convert pandas DataFrames to avro."""

import datetime
from io import BytesIO
from typing import Any, Dict, Generator, NamedTuple, Union

import fastavro
import numpy as np
import pandas as pd

from pyfxlib.lowlevel.pandasutil import (
    _column_dtype,
    to_avro_type,
)

_EXCEL_DATE_ORIGIN = 693594


def _encode_date_as_excel(date: datetime.date, *args: Any) -> int:
    return date.toordinal() - _EXCEL_DATE_ORIGIN


def _decode_date_as_excel(date: int, *args: Any) -> datetime.date:
    return datetime.date.fromordinal(date + _EXCEL_DATE_ORIGIN)


fastavro.write.LOGICAL_WRITERS["int-date"] = _encode_date_as_excel
fastavro.read.LOGICAL_READERS["int-date"] = _decode_date_as_excel


_BUFFER_SIZE = 1024**2 * 8


class AvroStream:
    """Readable Avro Stream."""

    def __init__(
        self,
        schema: Dict[str, Any],
        records_stream: Generator[Dict[str, Any], None, None],
    ) -> None:
        self._records = records_stream
        self._buffer = BytesIO()
        self._writer = fastavro.write.Writer(self._buffer, schema)
        self._writer.flush()
        self.len = self._buffer.tell()
        self._buffer.seek(0)

    @classmethod
    def from_dataframe(cls, dataframe: pd.DataFrame) -> "AvroStream":
        """Create an AvroStream from a pandas Dataframe."""
        return AvroStream(_infer_schema(dataframe), _as_records(dataframe))

    def read(
        self,
        size: int,
    ) -> bytes:
        """See `RawIOBase` corresponding method."""
        next_bytes = self._next_bytes_in_buffer(size)
        if self.len == 0:
            self._fill_buffer()
        return next_bytes

    def _next_bytes_in_buffer(self, size: int) -> bytes:
        max_readable = min(self.len, size if size >= 0 else self.len)
        read_bytes = self._buffer.read(max_readable)
        self.len -= len(read_bytes)
        return read_bytes

    def _fill_buffer(self) -> None:
        # reset buffer
        self._buffer.seek(0)
        self.len = 0

        try:
            while self.len < _BUFFER_SIZE:
                self._writer.write(next(self._records))
                self._writer.flush()
                self.len = self._buffer.tell()
        except StopIteration:
            ...
        self._buffer.seek(0)


def _infer_schema(data_frame: pd.DataFrame) -> Dict[str, Any]:
    def avro_type(column: pd.Series) -> Union[str, Dict[str, Any]]:
        avro_type = to_avro_type(column)
        if avro_type is not None:
            return avro_type
        raise ValueError(f"unsupported type for column '{column.name}': '{_column_dtype(column)}'")

    return {
        "type": "record",
        "name": "Root",
        "fields": [
            {"name": col_name, "type": ["null", avro_type(data_frame[col_name])]}
            for col_name in data_frame.columns
        ],
    }


def _as_records(df: pd.DataFrame) -> Generator[Dict[str, Any], None, None]:
    cols_with_na = df.columns[df.isna().any()]

    def to_pandavro_row(row: NamedTuple) -> Dict[str, Any]:
        record = row._asdict()
        for k, v in record.items():
            # Replace pd.NA with None so fastavro can write it
            if k in cols_with_na and pd.isna(v):
                record[k] = None
            elif isinstance(v, pd.Timestamp):
                # convert to epoch timestamp in milliseconds
                record[k] = int(v.asm8.astype("int64") / 10**6)
            elif isinstance(v, np.bool_):
                record[k] = bool(v)
        return record

    for row in df.itertuples(index=False):
        yield to_pandavro_row(row)
