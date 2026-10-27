"""Email notifications for pipeline runs.

Each run sends one summary email, not one per error. :func:`daily_summary`
builds it and :func:`send_email` sends it with the ``mail`` command.
"""

import subprocess

from .state import RunRecord

DEFAULT_RECIPIENTS = ("olynn@andrew.cmu.edu",)
"""Who gets pipeline emails unless the caller passes its own list."""

SUBJECT_PREFIX = "[alert-archive]"
"""Prefix on every email subject, for filtering."""


def send_email(subject: str, body: str, recipients=DEFAULT_RECIPIENTS, mail_command: str = "mail"):
    """Send an email with the ``mail`` command.

    ``mail`` exiting cleanly means the message was handed to the local mail
    system, not that it was delivered.

    Parameters
    ----------
    subject : str
        Subject line. :data:`SUBJECT_PREFIX` is added in front.
    body : str
        Plain-text body.
    recipients : sequence of str
        Email addresses.
    mail_command : str
        Program to run instead of ``mail``. It is called as
        ``<mail_command> -s <subject> <recipients...>`` with the body on stdin.

    Raises
    ------
    ValueError
        If ``recipients`` is empty.
    subprocess.CalledProcessError
        If the mail command fails.
    """
    if not recipients:
        raise ValueError("no email recipients")
    subprocess.run(
        [mail_command, "-s", f"{SUBJECT_PREFIX} {subject}", *recipients],
        input=body,
        text=True,
        check=True,
        capture_output=True,
    )


def daily_summary(
    record: RunRecord,
    failing: dict[str, tuple[str, str]] | None = None,
    format_change: dict | None = None,
) -> tuple[str, str]:
    """Build the summary email for a daily run.

    Parameters
    ----------
    record : RunRecord
        The run, as written to the ingested log.
    failing : dict[str, tuple[str, str]] or None
        Output of :meth:`PipelineState.failing_chunks` after the run was
        logged. Used to tell chunks failing for the first time from chunks
        that have kept failing.
    format_change : dict or None
        Details of the format change this run found, as passed to
        :meth:`PipelineState.flag_format_change`, or None if there was none.

    Returns
    -------
    tuple[str, str]
        The subject and body.
    """
    failing = failing or {}
    problems = []
    if format_change:
        problems.append("FORMAT CHANGE")
    if record.skipped:
        problems.append(f"{len(record.skipped)} chunk(s) skipped")
    status = ", ".join(problems) or "OK"
    subject = f"daily {record.run_id}: {status}"

    counts_text = ", ".join(
        f"{'unknown' if k is None else k}: {v}" for k, v in sorted(record.schema_id_counts.items(), key=str)
    )
    lines = [
        f"Run {record.run_id}, started {record.started}",
        f"Ingested {record.num_rows} alerts from {len(record.chunks)} chunk(s)"
        + (f" into {record.file_name}" if record.file_name else ""),
        f"Alerts per format: {counts_text or 'none'}",
    ]

    if format_change:
        lines += [
            "",
            "Format change (the flag is set; Saturday's re-import will skip until it's cleared):",
            *(f"  {k}: {v}" for k, v in format_change.items()),
        ]

    if record.skipped:
        lines += ["", "Skipped chunks (retried next run):"]
        for chunk, error in sorted(record.skipped.items()):
            since = failing.get(chunk, (record.started,))[0]
            note = "" if since == record.started else f" (failing since {since})"
            lines.append(f"  {chunk}{note}: {error}")

    return subject, "\n".join(lines) + "\n"
