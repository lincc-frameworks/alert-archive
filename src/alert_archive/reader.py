"""Read alert parquet chunks and reshape them for import into HATS.

The input files hold one alert per row, with ``diaSource`` and ``diaObject``
as struct columns and ``prvDiaSources`` / ``prvDiaForcedSources`` as lists of
structs. :class:`AlertReader` turns each table into the layout the catalog
uses:

- top-level ``ra`` / ``dec`` columns, copied from ``diaSource``;
- every struct and list-of-struct column as a struct of lists, which
  nested-pandas reads as a nested column;
- no cutout columns.
"""

import pyarrow as pa
import pyarrow.compute as pc
from hats_import.catalog.file_readers import ParquetPyarrowReader

COORDINATE_COLUMNS = ("ra", "dec")
"""Top-level coordinate columns added by the reader."""

COORDINATE_SOURCE = "diaSource"
"""Struct column the coordinates are copied from."""

CUTOUT_PREFIX = "cutout"
"""Columns whose names start with this prefix are dropped."""


class AlertReader(ParquetPyarrowReader):
    """Parquet reader for alert chunks.

    Takes the same arguments as
    :class:`hats_import.catalog.file_readers.ParquetPyarrowReader`.
    """

    def read(self, input_file, read_columns=None):
        """Read an alert parquet file in batches, reshaped for the catalog.

        Parameters
        ----------
        input_file : str or path-like
            Alert parquet file to read.
        read_columns : list[str] or None
            Output columns to return. hats-import asks for just ``ra`` and
            ``dec`` while mapping. The input files don't have those columns,
            so only ``diaSource.ra`` and ``diaSource.dec`` are read to
            compute them. If None, ``column_names`` is used, and if that is
            also None, all columns are returned.

        Yields
        ------
        pyarrow.Table
            One table per input batch.
        """
        columns = read_columns or self.column_names
        file_columns = _input_columns(columns) if columns else None
        for table in super().read(input_file, read_columns=file_columns):
            table = transform_alerts(table)
            yield table.select(columns) if columns else table


def _input_columns(columns):
    """Map requested output columns to the input columns needed to build them."""
    file_columns = [c for c in columns if c not in COORDINATE_COLUMNS]
    if len(file_columns) < len(columns) and COORDINATE_SOURCE not in file_columns:
        file_columns += [f"{COORDINATE_SOURCE}.{c}" for c in COORDINATE_COLUMNS]
    return file_columns


def transform_alerts(table: pa.Table) -> pa.Table:
    """Drop cutouts, add coordinates and convert struct columns to nested.

    Parameters
    ----------
    table : pyarrow.Table
        Alerts as stored in the input parquet files.

    Returns
    -------
    pyarrow.Table
        Alerts in the catalog's layout.
    """
    cutouts = [name for name in table.column_names if name.startswith(CUTOUT_PREFIX)]
    table = table.drop_columns(cutouts)
    table = add_coordinates(table)
    return to_nested(table)


def add_coordinates(table: pa.Table) -> pa.Table:
    """Add top-level ``ra`` and ``dec`` columns, copied from ``diaSource``.

    Parameters
    ----------
    table : pyarrow.Table
        Alerts with ``diaSource`` as a struct column.

    Returns
    -------
    pyarrow.Table
        The same table with ``ra`` and ``dec`` as its first two columns.
    """
    source = table.column(COORDINATE_SOURCE)
    for i, name in enumerate(COORDINATE_COLUMNS):
        table = table.add_column(i, name, pc.struct_field(source, name))
    return table


def to_nested(table: pa.Table) -> pa.Table:
    """Convert struct and list-of-struct columns into structs of lists.

    A struct becomes a struct of length-1 lists, so each alert has one nested
    row. A list of structs becomes a struct of lists of the same lengths. Null
    lists become empty lists, which matches the frozen catalog.

    Parameters
    ----------
    table : pyarrow.Table
        Alerts with struct and list-of-struct columns.

    Returns
    -------
    pyarrow.Table
        Alerts with those columns as structs of lists. Other columns are
        unchanged.
    """
    for i, field in enumerate(table.schema):
        if pa.types.is_struct(field.type):
            array = _struct_to_nested(table.column(i).combine_chunks())
        elif pa.types.is_list(field.type) and pa.types.is_struct(field.type.value_type):
            array = _list_of_struct_to_nested(table.column(i).combine_chunks())
        else:
            continue
        table = table.set_column(i, field.name, array)
    return table


def _struct_to_nested(array: pa.StructArray) -> pa.StructArray:
    offsets = pa.array(range(len(array) + 1), type=pa.int32())
    return _struct_of_lists(offsets, array)


def _list_of_struct_to_nested(array: pa.ListArray) -> pa.StructArray:
    # flatten() accounts for slicing and skips anything behind null lists,
    # so the offsets are rebuilt from the list lengths rather than reused.
    lengths = pc.fill_null(pc.list_value_length(array), 0)
    offsets = pa.concat_arrays([pa.array([0], type=pa.int32()), pc.cumulative_sum(lengths).cast(pa.int32())])
    return _struct_of_lists(offsets, array.flatten())


def _struct_of_lists(offsets: pa.Array, structs: pa.StructArray) -> pa.StructArray:
    # StructArray.flatten() (unlike .field()) applies the struct's own offset and validity.
    children = structs.flatten()
    return pa.StructArray.from_arrays(
        [pa.ListArray.from_arrays(offsets, child) for child in children],
        names=[f.name for f in structs.type],
    )
