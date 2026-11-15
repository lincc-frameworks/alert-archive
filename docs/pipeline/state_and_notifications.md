# State and notifications

The pipeline keeps its state in `rubin_alert_archive_v2/state/`, next to the
catalog rather than inside it, so tools reading the catalog never see it. The
code that reads and writes it is {py:mod}`alert_archive.state`.

## `ingested.jsonl`: the run log

One line per run, oldest first. A chunk in `landing/` counts as ingested once
some run lists it in `chunks`, and that's the only way the daily run decides
what's new. A real entry, with its chunk list shortened:

```json
{"run_id": "20261102T225212Z", "started": "2026-11-02T22:52:12.273689+00:00",
 "chunks": ["171141__00000000-00050000.parquet", "171141__00050000-00100000.parquet", "..."],
 "num_rows": 279959, "schema_id_counts": {"1101": 279959}, "skipped": {}, "consolidated": false}
```

| Field | Meaning |
|---|---|
| `run_id` | The run's start time in UTC. Its files in the catalog are named `<run_id>.parquet`. |
| `started` | The same time, in full. |
| `chunks` | The chunks it ingested. |
| `num_rows` | How many alerts it wrote. |
| `schema_id_counts` | Alerts per alert format, e.g. `1101`. |
| `skipped` | Chunks it couldn't read, with the error. The next run retries them. |
| `consolidated` | True if the run's alerts are in the consolidated `Npix=<pixel>.parquet` files rather than files of their own, as for the baseline import. |

A run is added to the log only after its files are written, and the log is
always replaced in one step, so a crash never leaves a half-written line. If a
run dies before logging itself, its chunks still count as new, and the next run
deletes its files and ingests them again.

## `format_change.json`: the format-change flag

The flag is set when this file exists. A run sets it when it finds alerts the
catalog can't take as they are: an alert format ID it doesn't know, or fields
that would have to be dropped, converted to another type, or emptied to fit the
catalog's columns. From then on every daily run stops
without ingesting anything until a person deletes it. The file records when
the flag was first set, and each run that hit a format change since, with what
changed:

```json
{
  "flagged_at": "<when it was first set>",
  "runs": [{"run_id": "...", "time": "...", "details": {"unknown_schema_ids": [1200]}}]
}
```

See [Runbook](runbook.md) for what to do when it's set.

## `lock`: the pipeline lock

The file exists while a job is writing to the catalog or the log, so two jobs
never overlap. It names the job holding it:

```json
{"job": "daily", "host": "sdfmilan123", "pid": 12345,
 "since": "2026-11-02T22:52:12+00:00", "slurm_job_id": "42806210"}
```

A job removes it when it finishes, even when it fails. If the job is killed
outright (preempted, cancelled, or out of time), the lock stays behind. The next
job asks Slurm about the job in `slurm_job_id`, and if it has ended, takes the
lock over and says so in its email. That's safe because every step of a run can
be recovered from a crash. A lock with no Slurm job ID (e.g. from a run on a
login node) is never taken over: a person has to check and delete it. See
[Runbook](runbook.md).

## The emails

Every daily run emails a summary. The subject starts with `[alert-archive]`, so
you can filter on it, then ✅ if the run had no problems, ⚠️ if it went fine
but needs a look, or ❌ if it had problems.

| Subject | When |
|---|---|
| `✅ daily <run_id>: OK` | The run ingested every new chunk, or there were none. |
| `❌ daily <run_id>: N chunk(s) skipped` | Some chunks had an unexpected name or a footer that couldn't be read. They're retried next run. The email says how long each one has been failing. |
| `⚠️ daily <run_id>: stale lock taken over` | A killed job had left the lock behind. The run went ahead, and the email names the killed job. |
| `❌ daily <run_id>: FORMAT CHANGE` | The run found a new alert format, ingested nothing and set the flag. |
| `❌ daily: not run, format-change flag set since <time>` | The flag was already set, so the run did nothing. |

If a run hits more than one of these, the subject lists them all and uses the
most serious mark.

The body lists how many alerts were ingested, from how many chunks, how many
were in each format, and any problems.

The emails go to `DEFAULT_RECIPIENTS` in {py:mod}`alert_archive.notify`. To
change who gets them, edit that list. They're sent with the `mail` command on
the node the job runs on.

A run that crashes sends no summary. Slurm's own job email (from
`--mail-user` on `sbatch`) is how you hear about those.
