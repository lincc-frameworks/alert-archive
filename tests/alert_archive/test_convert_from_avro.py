import gzip
import logging
import shutil

import nested_pandas as npd
import pyarrow.parquet as pq
import pytest
from alert_archive.convert_from_avro import (
    MANIFEST_NAME,
    UnknownFormatError,
    convert_chunk,
    decode_alert,
    read_manifest,
    update,
)
from alert_archive.schema import known_schema, target_schema


def statuses(results):
    """(name, status) of each chunk result."""
    return [(r.name, r.status) for r in results]


def converted_subdirs(out_dir):
    """The subdirectories in the manifest."""
    return [s["subdir"] for s in read_manifest(out_dir)["subdirs"]]


@pytest.mark.parametrize(
    ("path", "dia_source_id", "schema_id"),
    [
        ("170019/170019695985952427.avro.gz", 170019695985952427, 1000),
        ("170450/170450700567839112.avro.gz", 170450700567839112, 1100),
        ("171167/171167627158226197.avro.gz", 171167627158226197, 1101),
        ("171194/171194007958847495.avro.gz", 171194007958847495, 1102),
    ],
    ids=["1000", "1100", "1101", "1102"],
)
def test_decode_alert(avro_alerts, path, dia_source_id, schema_id):
    """A sample alert decodes to its format's fields, plus schemaId."""
    alert = decode_alert(avro_alerts / path)
    assert alert["diaSourceId"] == dia_source_id
    assert alert["schemaId"] == schema_id
    # Its format's fields, minus the cutouts, ssSource and mpc_orbits.
    assert set(alert) == {*known_schema(schema_id).names, "schemaId"}


def test_decode_alert_unknown_format(unknown_format_alert):
    """A format ID with no schema in the package raises, naming the ID."""
    with pytest.raises(UnknownFormatError) as info:
        decode_alert(unknown_format_alert)
    assert info.value.schema_id == 9999


@pytest.mark.parametrize("message", [b"\x01not confluent", b"\x00\x00\x00", b""])
def test_decode_alert_rejects_other_files(tmp_path, message):
    """A file that isn't in Confluent wire format, or is too short for its header, raises."""
    path = tmp_path / "x.avro.gz"
    path.write_bytes(gzip.compress(message))
    with pytest.raises(ValueError, match="Confluent"):
        decode_alert(path)


def test_convert_chunk_writes_catalog_columns(tmp_path, avro_alert_files):
    """A chunk has exactly the target schema, and each alert's own schemaId."""
    out = tmp_path / "chunk.parquet"
    result = convert_chunk(avro_alert_files, out, threads=2)
    assert (result.status, result.n_files, result.errors) == ("written", 4, [])

    assert pq.read_schema(out).equals(target_schema())
    chunk = npd.read_parquet(out)
    assert chunk["diaSourceId"].tolist() == [
        170019695985952427,
        170450700567839112,
        171167627158226197,
        171194007958847495,
    ]
    assert chunk["schemaId"].tolist() == [1000, 1100, 1101, 1102]
    # Fields new in 1101 and 1102 are kept, and null for the older formats.
    exposure_time = chunk["diaSource"].struct.field("exposureTime")
    assert exposure_time.isna().tolist() == [True, True, False, False]
    assert exposure_time.iloc[2] == pytest.approx(38.0, abs=1e-3)
    assert exposure_time.iloc[3] == pytest.approx(30.0, abs=1e-3)
    template_instrument = chunk["diaSource"].struct.field("templateInstrument")
    assert template_instrument.isna().tolist() == [True, True, True, False]
    assert template_instrument.iloc[3] == "LSSTCam"


def test_convert_chunk_fails_on_any_bad_file(tmp_path, avro_alert_files):
    """One bad file fails the chunk: nothing is written, not even a temporary file."""
    not_gzip, not_avro = tmp_path / "0.avro.gz", tmp_path / "9.avro.gz"
    not_gzip.write_bytes(b"not gzip")
    not_avro.write_bytes(gzip.compress(b"not avro"))
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    result = convert_chunk([not_gzip, *avro_alert_files, not_avro], out_dir / "chunk.parquet", threads=2)
    assert result.status == "failed"
    # Every bad file is reported, not only the first.
    assert len(result.errors) == 2
    assert "0.avro.gz" in result.errors[0] and "9.avro.gz" in result.errors[1]
    assert list(out_dir.iterdir()) == []


def test_convert_chunk_fails_on_any_unknown_format(tmp_path, avro_alert_files, unknown_format_alert):
    """One alert of an unknown format fails the chunk, and the error says what to do."""
    out = tmp_path / "chunk.parquet"
    result = convert_chunk([*avro_alert_files, unknown_format_alert], out, threads=2)
    assert result.status == "failed"
    assert "UnknownFormatError: Add schema of ID 9999" in result.errors[0]
    assert not out.exists()


def test_convert_chunk_leaves_existing_chunk_alone(tmp_path, avro_alert_files):
    """A chunk that exists already isn't converted again."""
    out = tmp_path / "chunk.parquet"
    out.write_bytes(b"from an earlier run")
    result = convert_chunk(avro_alert_files, out, threads=2)
    assert result.status == "exists"
    assert out.read_bytes() == b"from an earlier run"


def test_update(avro_alerts, avro_alerts_manifest, tmp_path):
    """update writes every subdirectory's chunks, and lists them in the manifest."""
    out = tmp_path / "landing"
    results = update(out, avro_alerts, threads=2)
    assert statuses(results) == [
        ("170019__00000000-00000001.parquet", "written"),
        ("170450__00000000-00000001.parquet", "written"),
        ("171167__00000000-00000001.parquet", "written"),
        ("171194__00000000-00000001.parquet", "written"),
    ]
    chunk = npd.read_parquet(out / "170019__00000000-00000001.parquet")
    assert chunk["diaSourceId"].tolist() == [170019695985952427]
    assert read_manifest(out) == avro_alerts_manifest


def test_update_converts_only_new_subdirs(avro_alerts, tmp_path):
    """Subdirectories in the manifest aren't looked at again, even if their chunks are gone."""
    out = tmp_path / "landing"
    update(out, avro_alerts, threads=2, chunk_size=2)
    (out / "170019__00000000-00000001.parquet").unlink()
    shutil.copytree(avro_alerts / "170019", avro_alerts / "new")

    results = update(out, avro_alerts, threads=2)
    assert statuses(results) == [("new__00000000-00000001.parquet", "written")]
    assert not (out / "170019__00000000-00000001.parquet").exists()
    assert read_manifest(out)["chunk_size"] == 2  # the manifest's, not the default


def test_update_with_nothing_new(avro_alerts, tmp_path):
    """With nothing new, nothing is converted and the manifest is unchanged."""
    out = tmp_path / "landing"
    update(out, avro_alerts, threads=2)
    manifest_before = (out / MANIFEST_NAME).read_text()
    assert update(out, avro_alerts, threads=2) == []
    assert (out / MANIFEST_NAME).read_text() == manifest_before


def test_update_retries_subdirs_with_a_failed_chunk(tmp_path, avro_alert_files):
    """A subdirectory with a failed chunk isn't recorded, and the next update retries the same files."""
    archive, out = tmp_path / "alerts", tmp_path / "landing"
    # One subdirectory with the 1000, 1100 and 1101 alerts: chunks [1000, 1100] and [1101],
    # with a chunk size of 2.
    a = archive / "a"
    a.mkdir(parents=True)
    for path in avro_alert_files[:3]:
        shutil.copy(path, a)
    first = a / "170019695985952427.avro.gz"
    alert = first.read_bytes()
    first.write_bytes(b"not gzip")  # unreadable for now
    results = update(out, archive, threads=2, chunk_size=2)
    assert statuses(results) == [
        ("a__00000000-00000002.parquet", "failed"),
        ("a__00000002-00000003.parquet", "written"),
    ]
    assert converted_subdirs(out) == []

    # The file is fixed, and an alert arrives that sorts between the first two.
    first.write_bytes(alert)
    shutil.copy(a / "170450700567839112.avro.gz", a / "170200000000000000.avro.gz")
    # The retry keeps the chunk size of 2, though nothing finished, and the same three files.
    # Listing "a" again would make the chunks [1000, new] and [1100, 1101], writing 1101 twice.
    results = update(out, archive, threads=2)
    assert statuses(results) == [
        ("a__00000000-00000002.parquet", "written"),
        ("a__00000002-00000003.parquet", "exists"),
    ]
    chunk = npd.read_parquet(out / "a__00000000-00000002.parquet")
    assert chunk["diaSourceId"].tolist() == [170019695985952427, 170450700567839112]
    assert converted_subdirs(out) == ["a"]
    assert not (out / "chunk_lists" / "a.txt").exists()


def test_update_skips_empty_subdirs_until_they_have_alerts(avro_alerts, tmp_path):
    """Empty subdirectories aren't recorded, so their alerts are converted once they arrive."""
    out = tmp_path / "landing"
    (avro_alerts / "empty").mkdir()
    update(out, avro_alerts, threads=2)
    assert converted_subdirs(out) == ["170019", "170450", "171167", "171194"]
    # Nothing is saved for the empty one.
    assert list((out / "chunk_lists").iterdir()) == []

    shutil.copy(avro_alerts / "170019" / "170019695985952427.avro.gz", avro_alerts / "empty")
    results = update(out, avro_alerts, threads=2)
    assert statuses(results) == [("empty__00000000-00000001.parquet", "written")]


def test_update_rejects_another_archive(avro_alerts, tmp_path):
    """update refuses an output directory made from a different archive."""
    out = tmp_path / "landing"
    update(out, avro_alerts, threads=2)
    with pytest.raises(ValueError, match="is for archive"):
        update(out, tmp_path / "elsewhere", threads=2)


def test_update_rejects_a_missing_archive(tmp_path):
    """update fails, rather than finding nothing new, if the archive isn't there."""
    with pytest.raises(FileNotFoundError, match="missing not found"):
        update(tmp_path / "landing", tmp_path / "missing", threads=2)


def test_update_logs_the_first_bad_files(avro_alerts, tmp_path, caplog):
    """A failed chunk's log names the chunk, its first 10 bad files, and how many more there are."""
    # 12 bad files: the log lists 10 of them, then "... and 2 more".
    for i in range(12):
        (avro_alerts / "170019" / f"bad{i:02d}.avro.gz").write_bytes(b"not gzip")
    with caplog.at_level(logging.WARNING, logger="alert_archive.convert_from_avro"):
        update(tmp_path / "landing", avro_alerts, threads=2)
    lines = [r.getMessage().strip() for r in caplog.records]
    assert len(lines) == 12
    assert lines[0] == ("170019__00000000-00000013.parquet: FAILED, 12 of 13 files failed to decode")
    assert lines[1].startswith(f"{avro_alerts}/170019/bad00.avro.gz: BadGzipFile")
    assert lines[10].startswith(f"{avro_alerts}/170019/bad09.avro.gz: BadGzipFile")
    assert lines[11] == "... and 2 more"
