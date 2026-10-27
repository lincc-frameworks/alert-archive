import subprocess

import pytest
from alert_archive.notify import OK_MARK, PROBLEM_MARK, SUBJECT_PREFIX, daily_summary, send_email
from alert_archive.state import RunRecord


@pytest.fixture
def fake_mail(tmp_path):
    """A stand-in for ``mail`` that saves its arguments and stdin to files."""
    script = tmp_path / "fake_mail"
    # A shell script, run by send_email in place of `mail`. The tests read the
    # two files it writes to check what send_email passed to the command,
    # without sending any email.
    # The \ after """ keeps #!/bin/sh on the first line, where it must be.
    script.write_text(
        f"""\
#!/bin/sh
echo "$@" > {tmp_path}/args   # its arguments: -s, subject, recipients
cat > {tmp_path}/body         # what was piped to stdin: the email body
"""
    )
    # New files aren't executable, and subprocess.run can only run a file
    # directly if it is. 0o755 = owner can read/write/execute, others read/execute.
    script.chmod(0o755)
    return script


def test_send_email(tmp_path, fake_mail):
    """The mail command gets the prefixed subject and recipients as args, and the body on stdin."""
    send_email(f"{OK_MARK} hello", "line 1\nline 2\n", ["a@x.org", "b@x.org"], mail_command=str(fake_mail))
    assert (tmp_path / "args").read_text() == f"-s {SUBJECT_PREFIX} {OK_MARK} hello a@x.org b@x.org\n"
    assert (tmp_path / "body").read_text() == "line 1\nline 2\n"


def test_send_email_fails_loudly(tmp_path):
    """A failing mail command or an empty recipient list raises."""
    with pytest.raises(subprocess.CalledProcessError):
        send_email("hello", "body", ["a@x.org"], mail_command="false")
    with pytest.raises(ValueError, match="recipients"):
        send_email("hello", "body", [])


def test_daily_summary_ok():
    """A clean run says OK and lists the rows and formats, with no problem sections."""
    record = RunRecord(
        run_id="r1",
        started="2026-10-27T03:00:00+00:00",
        file_name="r1.parquet",
        chunks=["a", "b"],
        num_rows=100,
        schema_id_counts={1100: 90, 1101: 10},
    )
    subject, body = daily_summary(record)
    assert subject == f"{OK_MARK} daily r1: OK"
    assert "Ingested 100 alerts from 2 chunk(s) into r1.parquet" in body
    assert "1100: 90, 1101: 10" in body
    assert "FORMAT CHANGE" not in body
    assert "SKIPPED CHUNKS" not in body


def test_daily_summary_sorts_formats_numerically():
    """Format IDs are listed in numeric order (999 before 1100), with unknown last."""
    record = RunRecord(run_id="r1", started="t1", schema_id_counts={None: 1, 1100: 2, 999: 3})
    _, body = daily_summary(record)
    assert "999: 3, 1100: 2, unknown: 1" in body


def test_daily_summary_reports_problems():
    """Format changes and skipped chunks are in the subject; only repeat failures say 'failing since'."""
    record = RunRecord(
        run_id="r3",
        started="t3",
        schema_id_counts={None: 5},
        skipped={"new.parquet": "truncated", "old.parquet": "corrupt"},
    )
    failing = {"new.parquet": ("t3", "truncated"), "old.parquet": ("t1", "corrupt")}
    subject, body = daily_summary(record, failing, format_change={"nulled": {"diaSource.x": "bad cast"}})

    assert subject == f"{PROBLEM_MARK} daily r3: FORMAT CHANGE, 2 chunk(s) skipped"
    assert "unknown: 5" in body
    assert "diaSource.x" in body
    assert "new.parquet: truncated" in body
    assert "old.parquet (failing since t1): corrupt" in body
