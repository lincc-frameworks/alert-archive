import shutil

from alert_archive.cli import main


def test_convert_from_avro(avro_alerts, unknown_format_alert, tmp_path, caplog):
    """convert-from-avro returns 1 if a chunk failed, so the Slurm job fails and emails."""
    args = [
        "convert-from-avro",
        "--out-dir",
        str(tmp_path / "landing"),
        "--archive-dir",
        str(avro_alerts),
        "--threads",
        "2",
    ]
    assert main(args) == 0
    assert caplog.messages[-1] == "done: 3 written, 0 already converted, 0 failed"

    (avro_alerts / "new").mkdir()
    shutil.copy(unknown_format_alert, avro_alerts / "new")
    assert main(args) == 1
    assert caplog.messages[-1] == "done: 0 written, 0 already converted, 1 failed"
