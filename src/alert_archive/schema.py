"""Alert formats and conversion of alert tables to the catalog's columns.

Rubin alerts are written in one of several Avro formats ("schemas"), each
identified by a numeric ID. Copies of the known formats live in
``alert_archive/schemas/<id>.json`` (from
``/sdf/data/rubin/shared/alert-archive/v2/schemas``). The catalog uses the
columns of the newest known format, minus :data:`EXCLUDED_COLUMNS`, plus a
``schemaId`` column.

:func:`conform_alerts` converts a table of alerts, as stored in the input
parquet files, to those columns. It never raises on unexpected data: it does
the best it can and returns a :class:`ConformReport` saying what it changed.
The caller decides what to do about it (see "Error handling" in
``docs/design/incremental_data_flow.md``).
"""

import json
from dataclasses import dataclass, field
from functools import cache
from importlib.resources import files

import pyarrow as pa
import pyarrow.compute as pc

SCHEMA_ID_COLUMN = "schemaId"
"""Column holding each alert's format ID."""

EXCLUDED_COLUMNS = ("ssSource", "mpc_orbits", "cutoutDifference", "cutoutScience", "cutoutTemplate")
"""Alert columns left out of the catalog.

``ssSource`` and ``mpc_orbits`` are left out for now because the input parquet
files don't have them (see the design doc). The cutouts are images we don't keep.
"""

AVRO_PRIMITIVES = {
    "boolean": pa.bool_(),
    "int": pa.int32(),
    "long": pa.int64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "string": pa.string(),
    "bytes": pa.binary(),
}
"""Arrow type for each Avro primitive type."""


# ---------------------------------------------------------------- Avro schemas


def avro_to_arrow(avro_schema: dict) -> pa.Schema:
    """Convert an Avro record schema to an Arrow schema.

    Supports only what the alert formats use: records, arrays, primitive
    types, ``["null", X]`` unions and references to records by name.

    Parameters
    ----------
    avro_schema : dict
        Parsed Avro schema of a record.

    Returns
    -------
    pyarrow.Schema
        One field per record field. ``["null", X]`` fields are nullable.

    Raises
    ------
    ValueError
        If the schema uses an Avro feature not listed above.
    """
    named = {}  # record name -> struct type, for references like array<diaSource>
    return pa.schema([_avro_field(f, named) for f in avro_schema["fields"]])


def _avro_field(avro_field: dict, named: dict) -> pa.Field:
    avro_type = avro_field["type"]
    nullable = isinstance(avro_type, list)
    if nullable:
        others = [t for t in avro_type if t != "null"]
        if len(avro_type) != 2 or len(others) != 1:
            raise ValueError(f"Unsupported Avro union for {avro_field['name']!r}: {avro_type}")
        avro_type = others[0]
    return pa.field(avro_field["name"], _avro_type(avro_type, named), nullable=nullable)


def _avro_type(avro_type, named: dict) -> pa.DataType:
    if isinstance(avro_type, str):
        if avro_type in AVRO_PRIMITIVES:
            return AVRO_PRIMITIVES[avro_type]
        if avro_type in named:
            return named[avro_type]
        raise ValueError(f"Unsupported or unknown Avro type: {avro_type!r}")
    if isinstance(avro_type, dict) and avro_type.get("type") == "array":
        return pa.list_(_avro_type(avro_type["items"], named))
    if isinstance(avro_type, dict) and avro_type.get("type") == "record":
        struct = pa.struct([_avro_field(f, named) for f in avro_type["fields"]])
        named[avro_type["name"]] = struct
        return struct
    raise ValueError(f"Unsupported Avro type: {avro_type}")


@cache
def known_schema_ids() -> tuple[int, ...]:
    """Format IDs with a schema file in the package, in increasing order."""
    names = (p.name for p in files("alert_archive.schemas").iterdir())
    return tuple(sorted(int(n.removesuffix(".json")) for n in names if n.endswith(".json")))


@cache
def known_schema(schema_id: int) -> pa.Schema:
    """Arrow schema of a known alert format, minus :data:`EXCLUDED_COLUMNS`.

    Parameters
    ----------
    schema_id : int
        One of :func:`known_schema_ids`.

    Returns
    -------
    pyarrow.Schema
        The format's columns, with Avro's nullability.
    """
    text = files("alert_archive.schemas").joinpath(f"{schema_id}.json").read_text()
    schema = avro_to_arrow(json.loads(text))
    return pa.schema([f for f in schema if f.name not in EXCLUDED_COLUMNS])


@cache
def target_schema() -> pa.Schema:
    """Columns of the catalog's input tables, before :mod:`alert_archive.reader` reshapes them.

    The newest known format's columns, with ``schemaId`` after
    ``diaSourceId`` and every field nullable at every level. Nullability is
    dropped because a best-effort conversion can produce nulls anywhere, and
    the reader's nesting step doesn't keep it anyway.

    Returns
    -------
    pyarrow.Schema
        The target schema.
    """
    fields = [_nullable(f) for f in known_schema(known_schema_ids()[-1])]
    fields.insert(1, pa.field(SCHEMA_ID_COLUMN, pa.int32()))
    return pa.schema(fields)


def _nullable(f: pa.Field) -> pa.Field:
    if pa.types.is_struct(f.type):
        return pa.field(f.name, pa.struct([_nullable(c) for c in f.type]))
    if pa.types.is_list(f.type):
        return pa.field(f.name, pa.list_(_nullable(f.type.value_field).type))
    return f.with_nullable(True)


# ---------------------------------------------------------------- format IDs


def infer_schema_id(schema: pa.Schema) -> int | None:
    """Guess the format of alerts from their columns.

    Stands in for a real per-alert ``schemaId`` until the converter writes
    one. The columns (names and types at every level, ignoring nullability)
    are compared with each known format. 1000 and 1100 differ only in
    nullability, so they can't be told apart, and the newest match (1100) is
    returned.

    Parameters
    ----------
    schema : pyarrow.Schema
        Schema of an input table. :data:`EXCLUDED_COLUMNS` and ``schemaId``
        are ignored.

    Returns
    -------
    int or None
        The newest known format with exactly these columns, or None if no
        known format matches.
    """
    columns = _leaf_types(
        pa.schema([f for f in schema if f.name not in (*EXCLUDED_COLUMNS, SCHEMA_ID_COLUMN)])
    )
    matches = [i for i in known_schema_ids() if _leaf_types(known_schema(i)) == columns]
    return matches[-1] if matches else None


def _leaf_types(schema: pa.Schema) -> dict[str, pa.DataType]:
    """Map each leaf's dotted path to its type. List levels don't add to the path."""
    leaves = {}

    def walk(path, dtype):
        if pa.types.is_list(dtype):
            walk(path, dtype.value_type)
        elif pa.types.is_struct(dtype):
            for child in dtype:
                walk(f"{path}.{child.name}", child.type)
        else:
            leaves[path] = dtype

    for f in schema:
        walk(f.name, f.type)
    return leaves


# ---------------------------------------------------------------- conversion


@dataclass
class ConformReport:
    """What :func:`conform_alerts` changed to fit a table to the catalog's columns.

    Field paths are dotted, e.g. ``"diaSource.exposureTime"``. Paths inside
    lists of structs leave out the list level, e.g. ``"prvDiaSources.snr"``.
    """

    schema_ids: dict[int | None, int] = field(default_factory=dict)
    """Number of alerts per format ID. None means the format is unknown."""
    inferred_schema_id: bool = False
    """True if the input had no ``schemaId`` column and it was inferred from the columns."""
    dropped: list[str] = field(default_factory=list)
    """Input fields the catalog doesn't have. Their values are discarded."""
    filled: list[str] = field(default_factory=list)
    """Catalog fields missing from the input, filled with nulls. Expected for older formats."""
    cast: list[str] = field(default_factory=list)
    """Fields whose type differed and were converted successfully."""
    nulled: dict[str, str] = field(default_factory=dict)
    """Fields whose type differed and couldn't be converted, set to null, with the error."""

    @property
    def unknown_schema_ids(self) -> list[int | None]:
        """Format IDs in the input that aren't known. None if a format couldn't be inferred."""
        known = known_schema_ids()
        return [i for i in self.schema_ids if i not in known]

    @property
    def is_format_change(self) -> bool:
        """True if the input looks like a format we don't fully support.

        That is: an unknown format ID, or any field dropped, converted or set
        to null. Fields filled with nulls alone don't count, since older
        known formats lack some newer fields.
        """
        return bool(self.unknown_schema_ids or self.dropped or self.cast or self.nulled)


def conform_alerts(table: pa.Table) -> tuple[pa.Table, ConformReport]:
    """Convert a table of alerts to the catalog's columns, as well as possible.

    Missing fields are filled with nulls, fields the catalog doesn't have
    are dropped, and fields of a different type are converted, or set to
    null if they can't be. :data:`EXCLUDED_COLUMNS` are dropped without being
    reported. If there's no ``schemaId`` column, one is added with the
    format inferred by :func:`infer_schema_id`.

    Parameters
    ----------
    table : pyarrow.Table
        Alerts as stored in the input parquet files.

    Returns
    -------
    pyarrow.Table
        The alerts with exactly :func:`target_schema`'s columns.
    ConformReport
        What was changed.
    """
    report = ConformReport()
    target = target_schema()

    if SCHEMA_ID_COLUMN not in table.column_names:
        report.inferred_schema_id = True
        schema_id = infer_schema_id(table.schema)
        table = table.append_column(SCHEMA_ID_COLUMN, pa.array([schema_id] * len(table), type=pa.int32()))

    extra = [n for n in table.column_names if n not in target.names and n not in EXCLUDED_COLUMNS]
    report.dropped.extend(extra)

    columns = []
    for target_field in target:
        if target_field.name in table.column_names:
            array = table.column(target_field.name).combine_chunks()
            columns.append(_conform(array, target_field.type, target_field.name, report))
        else:
            report.filled.append(target_field.name)
            columns.append(pa.nulls(len(table), type=target_field.type))
    result = pa.Table.from_arrays(columns, schema=target)

    counts = pc.value_counts(result.column(SCHEMA_ID_COLUMN).combine_chunks())
    report.schema_ids = {c["values"].as_py(): c["counts"].as_py() for c in counts}
    return result, report


def _conform(array: pa.Array, target: pa.DataType, path: str, report: ConformReport) -> pa.Array:
    """Convert one array to the target type, recording what was changed."""
    if pa.types.is_struct(target) and pa.types.is_struct(array.type):
        return _conform_struct(array, target, path, report)
    if pa.types.is_list(target) and pa.types.is_list(array.type):
        values = _conform(array.values, target.value_type, path, report)
        return pa.ListArray.from_arrays(array.offsets, values, type=target, mask=_null_mask(array))
    if array.type == target:
        return array
    try:
        converted = array.cast(target)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as exc:
        report.nulled[path] = f"{array.type} -> {target}: {exc}"
        return pa.nulls(len(array), type=target)
    report.cast.append(path)
    return converted


def _conform_struct(array: pa.StructArray, target: pa.StructType, path: str, report: ConformReport):
    names = {f.name for f in array.type}
    report.dropped.extend(f"{path}.{n}" for n in sorted(names - {f.name for f in target}))
    children = []
    for child in target:
        child_path = f"{path}.{child.name}"
        if child.name in names:
            # pc.struct_field (unlike StructArray.field) accounts for slicing.
            children.append(_conform(pc.struct_field(array, child.name), child.type, child_path, report))
        else:
            report.filled.append(child_path)
            children.append(pa.nulls(len(array), type=child.type))
    return pa.StructArray.from_arrays(children, fields=list(target), mask=_null_mask(array))


def _null_mask(array: pa.Array) -> pa.Array | None:
    return array.is_null() if array.null_count else None
