"""Read alert parquet chunks and reshape them for import into HATS.

The input files hold one alert per row, with ``diaSource`` and ``diaObject``
as struct columns and ``prvDiaSources`` / ``prvDiaForcedSources`` as lists of
structs. :class:`AlertReader` turns each table into the catalog's columns
and layout:

- the catalog's columns, from :func:`alert_archive.schema.conform_alerts`;
  any format change stops the read with :class:`FormatChangeError`;
- top-level ``ra`` / ``dec`` columns, copied from ``diaSource``;
- every struct and list-of-struct column as a struct of lists, which
  nested-pandas reads as a nested column;
- no cutout columns.
"""

import pyarrow as pa
import pyarrow.compute as pc
from hats.io import file_io
from hats_import.catalog.file_readers import ParquetPyarrowReader

from .schema import ConformReport, conform_alerts

COORDINATE_COLUMNS = ("ra", "dec")
"""Top-level coordinate columns added by the reader."""

COORDINATE_SOURCE = "diaSource"
"""Struct column the coordinates are copied from."""

CUTOUT_PREFIX = "cutout"
"""Columns whose names start with this prefix are dropped."""

DEFAULT_CHUNKSIZE = 5_000
"""Rows per batch. Alerts in busy fields carry long histories: one 50k-row
dump chunk takes about 31 GB to read whole, but 8 GB in batches of this size."""


class FormatChangeError(ValueError):
    """Raised when an input chunk's alerts aren't in a format the catalog fully supports."""

    def __init__(self, input_file, report: ConformReport):
        self.input_file = input_file
        self.report = report
        super().__init__(
            f"{input_file}: alerts don't fit the catalog's format. Format IDs: {report.schema_ids}. "
            f"Dropped: {report.dropped}. Cast: {report.cast}. Set to null: {list(report.nulled)}."
        )


class AlertReader(ParquetPyarrowReader):
    """Parquet reader for alert chunks.

    Takes the same arguments as
    :class:`hats_import.catalog.file_readers.ParquetPyarrowReader`.

    Parameters
    ----------
    chunksize : int
        Rows per batch (see :data:`DEFAULT_CHUNKSIZE`). Each batch is
        converted on its own, so the result doesn't depend on it.
    column_names : list[str] or None
        Output columns to return. If None (the default), all columns are
        returned. If given, it must include ``ra`` and ``dec``: hats-import
        places each alert on the sky by them, and without them the import
        fails partway through, after reading every input file once.
        Every read returns the catalog's columns: converting needs the whole
        alert, so a subset is read in full and selected afterwards. The one
        exception is reading only ``ra`` / ``dec``, as hats-import's mapping
        pass does, which reads just those two fields.

    Raises
    ------
    ValueError
        If ``column_names`` is given without ``ra`` and ``dec``.
    """

    def __init__(self, chunksize=DEFAULT_CHUNKSIZE, column_names=None, iterate_by_row_groups=False, **kwargs):
        if column_names is not None:
            missing = [c for c in COORDINATE_COLUMNS if c not in column_names]
            if missing:
                raise ValueError(
                    f"column_names must include the coordinate columns {list(COORDINATE_COLUMNS)} "
                    f"(missing: {missing}). hats-import needs them to place each alert on the sky."
                )
        super().__init__(
            chunksize=chunksize,
            column_names=column_names,
            iterate_by_row_groups=iterate_by_row_groups,
            **kwargs,
        )

    def read(self, input_file, read_columns=None):
        """Read an alert parquet file in batches, reshaped for the catalog.

        Parameters
        ----------
        input_file : str or path-like
            Alert parquet file to read.
        read_columns : list[str] or None
            Output columns to return. hats-import asks for just ``ra`` and
            ``dec`` while mapping. The input files don't have those columns,
            so ``diaSource.ra`` and ``diaSource.dec`` are read to compute them.
            If None, ``column_names`` is used, and if that is also None, all
            columns are returned.

        Yields
        ------
        pyarrow.Table
            One table per input batch.

        Raises
        ------
        FormatChangeError
            If a batch's alerts aren't in a format the catalog fully supports
            (see :attr:`~alert_archive.schema.ConformReport.is_format_change`).
        """
        columns = read_columns or self.column_names
        coordinates_only = bool(columns) and set(columns) <= set(COORDINATE_COLUMNS)
        if coordinates_only:
            file_columns = [f"{COORDINATE_SOURCE}.{c}" for c in COORDINATE_COLUMNS]
        else:
            # Converting to the catalog's columns needs the whole alert, so read everything.
            file_columns = file_io.read_parquet_file(input_file, **self.kwargs).schema_arrow.names
        for table in super().read(input_file, read_columns=file_columns):
            if not coordinates_only:
                table, report = conform_alerts(table)
                if report.is_format_change:
                    raise FormatChangeError(input_file, report)
            table = transform_alerts(table)
            yield table.select(columns) if columns else table


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
    for i, col_name in enumerate(COORDINATE_COLUMNS):
        table = table.add_column(i, col_name, pc.struct_field(source, col_name))
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
    """Turn a struct column into a struct of length-1 lists.

    Each row's struct becomes one nested row, e.g. ``{a: 1, b: 2}`` becomes
    ``{a: [1], b: [2]}``.
    """
    offsets = pa.array(range(len(array) + 1), type=pa.int32())
    return _struct_of_lists(offsets, array)


def _list_of_struct_to_nested(array: pa.ListArray) -> pa.StructArray:
    """Turn a list-of-struct column into a struct of lists.

    Each row's list of structs becomes one row of per-field lists of the same
    length, e.g. ``[{a: 1}, {a: 2}]`` becomes ``{a: [1, 2]}``. A null list
    becomes empty lists, ``{a: []}``.
    """
    # flatten() accounts for slicing and skips anything behind null lists,
    # so the offsets are rebuilt from the list lengths rather than reused.
    lengths = pc.fill_null(pc.list_value_length(array), 0)
    offsets = pa.concat_arrays([pa.array([0], type=pa.int32()), pc.cumulative_sum(lengths).cast(pa.int32())])
    return _struct_of_lists(offsets, array.flatten())


def _struct_of_lists(offsets: pa.Array, structs: pa.StructArray) -> pa.StructArray:
    """Group a flat run of structs into rows, one list per struct field.

    Row ``i`` holds ``structs[offsets[i]:offsets[i + 1]]``, split by field: for
    each field of the structs, a list of that field's values over those
    entries.

    Parameters
    ----------
    offsets : pyarrow.Array
        int32 row boundaries into ``structs``: ``len(rows) + 1`` values,
        starting at 0 and ending at ``len(structs)``.
    structs : pyarrow.StructArray
        All entries for all rows, back to back.

    Returns
    -------
    pyarrow.StructArray
        One row per pair of consecutive offsets, with a list per field.
    """
    # StructArray.flatten() (unlike .field()) applies the struct's own offset and validity.
    children = structs.flatten()
    return pa.StructArray.from_arrays(
        [pa.ListArray.from_arrays(offsets, child) for child in children],
        names=[f.name for f in structs.type],
    )
