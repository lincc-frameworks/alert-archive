import json

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from alert_archive.schema import (
    EXCLUDED_COLUMNS,
    SCHEMA_ID_COLUMN,
    avro_schema_to_arrow,
    conform_alerts,
    infer_schema_id,
    known_schema,
    known_schema_ids,
    target_schema,
)

NEW_IN_1101 = ["exposureTime", "trailAlgorithm", "trail_flag", "reliabilityVersion"]
"""diaSource fields added in format 1101, in the order they appear in the schema."""


def as_json(table):
    """Values of a table as JSON, for comparisons where NaN must equal NaN."""
    return json.dumps(table.to_pylist())


def with_struct_field(table, column, name, values):
    """Copy of `table` where struct `column` has field `name` set to `values` (added if missing)."""
    struct = table.column(column).combine_chunks()
    fields = [f for f in struct.type if f.name != name]
    children = [pc.struct_field(struct, f.name) for f in fields]
    values = pa.array(values)
    fields.append(pa.field(name, values.type))
    children.append(values)
    new = pa.StructArray.from_arrays(children, fields=fields)
    return table.set_column(table.column_names.index(column), column, new)


# ---------------------------------------------------------------- avro_schema_to_arrow


def test_avro_schema_to_arrow_types():
    """Primitive types, nullable unions, arrays, records and references by name."""
    avro = {
        "type": "record",
        "name": "alert",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "flux", "type": ["null", "float"]},
            {
                "name": "src",
                "type": {"type": "record", "name": "src", "fields": [{"name": "ok", "type": "boolean"}]},
            },
            {"name": "prv", "type": ["null", {"type": "array", "items": "src"}]},
        ],
    }
    src = pa.struct([pa.field("ok", pa.bool_(), nullable=False)])
    expected = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("flux", pa.float32()),
            pa.field("src", src, nullable=False),
            pa.field("prv", pa.list_(src)),
        ]
    )
    assert avro_schema_to_arrow(avro).equals(expected)


@pytest.mark.parametrize(
    "avro_type",
    [
        {"type": "map", "values": "int"},
        {"type": "enum", "name": "band", "symbols": ["g", "r"]},
        ["null", "int", "string"],
        ["int", "string"],
        "notAType",
    ],
)
def test_avro_schema_to_arrow_rejects_unsupported(avro_type):
    """Anything outside the supported subset raises instead of guessing a type."""
    with pytest.raises(ValueError):
        avro_schema_to_arrow(
            {"type": "record", "name": "alert", "fields": [{"name": "x", "type": avro_type}]}
        )


# ---------------------------------------------------------------- known formats


def test_known_schema_ids():
    """One ID per schema file in the package."""
    assert known_schema_ids() == (1000, 1100, 1101)


def test_known_schema_1101():
    """1101 adds four diaSource fields, which prvDiaSources gets too; excluded columns are gone."""
    schema = known_schema(1101)
    dia_source = schema.field("diaSource").type
    assert dia_source.num_fields == 102
    assert schema.field("prvDiaSources").type.value_type == dia_source
    assert all(dia_source.get_field_index(name) >= 0 for name in NEW_IN_1101)
    assert not set(EXCLUDED_COLUMNS) & set(schema.names)


def test_target_schema():
    """1101's columns plus schemaId, nullable at every level, with 1101's field order."""
    target = target_schema()
    assert target.names[:2] == ["diaSourceId", SCHEMA_ID_COLUMN]
    assert target.field(SCHEMA_ID_COLUMN).type == pa.int32()
    assert not set(EXCLUDED_COLUMNS) & set(target.names)

    dia_source = target.field("diaSource").type
    names = [f.name for f in dia_source]
    assert names[names.index("midpointMjdTai") + 1] == "exposureTime"

    def all_nullable(f):
        if pa.types.is_struct(f.type):
            return f.nullable and all(all_nullable(c) for c in f.type)
        if pa.types.is_list(f.type):
            return f.nullable and all_nullable(f.type.value_field)
        return f.nullable

    assert all(all_nullable(f) for f in target)


# ---------------------------------------------------------------- infer_schema_id


def test_infer_schema_id_dump(raw_alerts):
    """The dump's columns match 1000 and 1100 exactly; the newest match wins."""
    assert infer_schema_id(raw_alerts.schema) == 1100


def test_infer_schema_id_1101():
    """A table with the 1101 columns is recognized as 1101."""
    assert infer_schema_id(target_schema()) == 1101


def test_infer_schema_id_unknown(raw_alerts):
    """An extra field or a changed type matches no known format."""
    extra = with_struct_field(raw_alerts, "diaSource", "brandNewField", [1.0] * len(raw_alerts))
    assert infer_schema_id(extra.schema) is None
    retyped = with_struct_field(raw_alerts, "diaSource", "snr", ["high"] * len(raw_alerts))
    assert infer_schema_id(retyped.schema) is None


# ---------------------------------------------------------------- conform_alerts


def test_conform_dump(raw_alerts):
    """Dump alerts get the target columns; only the 1101 fields are filled with nulls."""
    table, report = conform_alerts(raw_alerts)
    assert table.schema.equals(target_schema())
    assert report.inferred_schema_id
    assert report.schema_ids == {1100: len(raw_alerts)}
    expected_filled = [f"{c}.{n}" for c in ["diaSource", "prvDiaSources"] for n in NEW_IN_1101]
    assert sorted(report.filled) == sorted(expected_filled)
    assert report.dropped == report.cast == []
    assert report.nulled == {}
    assert not report.is_format_change


def test_conform_keeps_values(raw_alerts):
    """Apart from the added fields, every value is unchanged, including null histories."""
    table, _ = conform_alerts(raw_alerts)
    rows = table.select(raw_alerts.column_names).to_pylist()
    for row in rows:
        for name in NEW_IN_1101:
            del row["diaSource"][name]
            for prv in row["prvDiaSources"] or []:
                del prv[name]
    assert json.dumps(rows) == as_json(raw_alerts)
    assert table.column("prvDiaSources").null_count == raw_alerts.column("prvDiaSources").null_count > 0


def test_conform_1101_round_trip(raw_alerts):
    """Converted alerts without schemaId are recognized as 1101 and need no changes."""
    once, _ = conform_alerts(raw_alerts)
    twice, report = conform_alerts(once.drop_columns([SCHEMA_ID_COLUMN]))
    assert report.schema_ids == {1101: len(raw_alerts)}
    assert report.filled == report.dropped == report.cast == []
    assert as_json(twice.drop_columns([SCHEMA_ID_COLUMN])) == as_json(once.drop_columns([SCHEMA_ID_COLUMN]))


def test_conform_uses_schema_id_column(raw_alerts):
    """A real schemaId column is kept, and unknown IDs are reported."""
    ids = [1100] * 6 + [1200] * 6
    table, report = conform_alerts(raw_alerts.append_column(SCHEMA_ID_COLUMN, pa.array(ids, pa.int32())))
    assert not report.inferred_schema_id
    assert report.schema_ids == {1100: 6, 1200: 6}
    assert report.unknown_schema_ids == [1200]
    assert report.is_format_change
    assert table.column(SCHEMA_ID_COLUMN).to_pylist() == ids


def test_conform_drops_unknown_fields(raw_alerts):
    """Unknown columns and struct fields are dropped and reported; excluded columns aren't reported."""
    table = with_struct_field(raw_alerts, "diaSource", "brandNewField", [1.0] * len(raw_alerts))
    table = table.append_column("brandNewColumn", pa.array([1] * len(table)))
    table = table.append_column("ssSource", pa.nulls(len(table), pa.struct([("x", pa.int32())])))
    result, report = conform_alerts(table)
    assert result.schema.equals(target_schema())
    assert sorted(report.dropped) == ["brandNewColumn", "diaSource.brandNewField"]
    assert report.unknown_schema_ids == [None]
    assert report.is_format_change


def test_conform_fills_missing_columns(raw_alerts):
    """A missing column is filled with nulls; its absence matches no known format, so it's flagged."""
    result, report = conform_alerts(raw_alerts.drop_columns(["diaObject"]))
    assert result.schema.equals(target_schema())
    assert "diaObject" in report.filled
    assert result.column("diaObject").null_count == len(result)
    assert report.unknown_schema_ids == [None]
    assert report.is_format_change


def test_conform_casts_or_nulls_changed_types(raw_alerts):
    """A changed type is converted if possible, otherwise set to null; other fields are untouched."""
    detectors = pc.struct_field(raw_alerts["diaSource"], "detector").cast(pa.int64())
    table = with_struct_field(raw_alerts, "diaSource", "detector", detectors)
    table = with_struct_field(table, "diaSource", "snr", ["high"] * len(table))
    result, report = conform_alerts(table)

    assert report.cast == ["diaSource.detector"]
    assert list(report.nulled) == ["diaSource.snr"]
    assert report.is_format_change
    dia_source = result.column("diaSource")
    assert pc.struct_field(dia_source, "snr").null_count == len(result)
    expected_detectors = pc.struct_field(raw_alerts["diaSource"], "detector").to_pylist()
    assert pc.struct_field(dia_source, "detector").to_pylist() == expected_detectors
    ra = pc.struct_field(raw_alerts["diaSource"], "ra")
    assert pc.struct_field(dia_source, "ra").to_pylist() == ra.to_pylist()


def test_conform_sliced_and_chunked(raw_alerts):
    """Slices and multi-chunk columns give the same result as converting the whole table."""
    full, _ = conform_alerts(raw_alerts)
    sliced, _ = conform_alerts(raw_alerts.slice(3, 6))
    assert as_json(sliced) == as_json(full.slice(3, 6))
    chunked, _ = conform_alerts(pa.concat_tables([raw_alerts.slice(0, 5), raw_alerts.slice(5)]))
    assert as_json(chunked) == as_json(full)
