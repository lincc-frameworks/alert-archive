# Incremental alert archive: data flow

Draft design for nightly updates to the alert archive HATS catalog.
Weekly re-import and publishing are tracked in
[lsdb#1536](https://github.com/astronomy-commons/lsdb/issues/1536).

```mermaid
flowchart TD
    %% ---------- sources ----------
    subgraph SRC["Alert source"]
        LIVE[("Live alert archive<br/>Avro .avro.gz, 1 file per alert<br/><i>new path TBD</i>")]
        DUMP[("Existing parquet dump<br/>(Neven's 50k-alert chunks)")]
    end

    %% ---------- black box ----------
    BB["Avro → parquet converter<br/>(our copy of Neven's script, refactored)<br/>emits only new alerts, keeps all fields,<br/>adds schemaId from the Avro header"]
    LAND[("Landing dir<br/>parquet batches + manifest")]

    LIVE --> BB --> LAND
    DUMP -.->|"replay chunk by chunk<br/>(backfill + mock)"| LAND

    %% ---------- bootstrap ----------
    BASE["Baseline build: full hats-import<br/>of the first chunks (notebook code)"]
    DUMP --> BASE
    WORK[("New working catalog<br/>Npix=N/ directories, 1101 columns<br/>main table only, grows nightly")]
    BASE --> WORK

    %% ---------- daily ----------
    subgraph DAILY["Daily pipeline (this repo)"]
        D1["Find unprocessed batches<br/>(compare with ingested-file list)"]
        D2["AlertReader transform<br/>add ra/dec, struct → nested, drop cutouts"]
        D3["Cast to catalog columns (1101)<br/>best effort for unknown formats"]
        D4["Write Npix=N/&lt;run_id&gt;.parquet,<br/>update skymaps + metadata,<br/>add batches to ingested list<br/>(ppdb_hats increment.py)"]
        D1 --> D2 --> D3 --> D4
        D3 -->|"unknown format"| FLAG["Set format-change flag<br/>+ email"]
    end

    LAND --> D1
    D4 --> WORK

    %% ---------- weekly (lsdb#1536) ----------
    subgraph WEEKLY["Weekly re-import, Saturday (lsdb#1536)"]
        W0{"Format-change<br/>flag set?"}
        W1["Merge each leaf dir's nightly files,<br/>rebalance, rebuild margin + diaSourceId index"]
        W2["Validate: row counts and<br/>per-object source counts"]
        W3["Atomic promote<br/>(keep the previous version for rollback)"]
        SKIP["Skip this week + email"]
        W0 -->|no| W1 --> W2 --> W3
        W0 -->|yes| SKIP
    end

    WORK --> W0
    FLAG -.-> W0
    HUMAN["Human: add the new format,<br/>re-ingest affected nights,<br/>clear the flag, re-run weekly"]
    FLAG --> HUMAN
    SKIP --> HUMAN
    HUMAN -.-> W0

    PUB[("Published collection<br/>lsdb_commissioning/rubin_alert_archive_v2/")]
    W3 --> PUB
    PUB --> USERS["Users, nightly benchmarks"]

    %% ---------- styling ----------
    classDef reuse fill:#d7ecd9,stroke:#2e7d32,color:#000
    classDef new fill:#dbe7f7,stroke:#1f5fa8,color:#000
    classDef tbd fill:#fbe6c9,stroke:#b26a00,color:#000,stroke-dasharray:4 3
    classDef fmt fill:#f6d5d5,stroke:#b3261e,color:#000
    class BASE,D2,D4 reuse
    class D1,D3,BB new
    class FLAG,SKIP,HUMAN,W0 fmt
    class LIVE tbd
```

Key: green = existing code (from the notebook or ppdb_hats), blue = new code
for this repo, red = alert-format-change handling, orange dashed = not settled yet.

## Decisions

### Catalog and storage

- **The frozen catalog is not touched or reused.** The new catalog is rebuilt
  in a new directory from the parquet dump: a baseline hats-import of the
  first chunks, then the remaining chunks replayed through the daily pipeline.
  The replay is also the mock source until live alerts flow, and it tests the
  increment code on real alerts. The dump has 20.6M rows, against 15.3M in the
  frozen catalog. Its files fall into four write dates: 347 from Jun 26 (likely
  the frozen catalog's input, so a natural baseline), then 87 from Jul 13,
  10 from Jul 21 and 66 from Jul 31 (about 5.3M newer alerts to replay).
- **Location:** `/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_v2/`,
  holding a `working/` catalog plus the published versions that lsdb#1536
  needs for promote and rollback.
- **Pipeline state** lives in `rubin_alert_archive_v2/state/`, outside the
  catalog so it survives the weekly re-import:
  - `ingested.jsonl`: one line per daily run, with the run ID, its start
    time, the parquet file name it wrote, its input chunks, its row count and
    its `schemaId` counts. It also lists the chunks the run skipped, with their
    errors, so the summary email can say how long a chunk has been failing.
    Only the input chunks count as ingested. Nightly files are named by run ID,
    not date, so a manual re-run can't overwrite an earlier run's files.
  - `format_change.json`: present only while flagged. Its existence is the flag.
  - `lock`: an atomic lock file so the daily and weekly jobs never overlap.
    If a job dies without removing it, nothing breaks it automatically: the
    next job fails loudly until a person checks and deletes it.

### Input and alert formats

- **The converter lives in this package.** It's our own copy of Neven's
  Avro → parquet script, refactored as needed. It is responsible for the
  "only new alerts" guarantee, for keeping every field, and for writing a
  per-alert `schemaId` column read from the Avro header (bytes 1–4).
- **`schemaId` is mocked for now.** Neven's dump has no format ID and uses
  one uniform column set: no 1101 fields, and every field nullable. So the
  mock infers it from the columns present: `1101` if any of the four 1101
  `diaSource` fields exist, otherwise `1100`. 1000 and 1100 differ only in
  nullability, so the dump can't tell them apart. It doesn't matter here,
  because both cast to the catalog the same way. The real value replaces the
  mock once the converter rewrite lands. The 170xxx raw Avro files are still on
  disk, so the real IDs could be recovered later if needed.
- **Format 1101 is supported.** The new catalog uses the 1101 columns. Rows
  from 1000/1100 alerts get nulls for the four new `diaSource` fields
  (`exposureTime`, `reliabilityVersion`, `trailAlgorithm`, `trail_flag`).
- **Unknown alert formats:** the daily pipeline keeps running and writes those
  alerts as well as it can. It also sets a format-change flag and sends an email.
  While the flag is set, the Saturday re-import skips aggregation and sends an email.
  A person then adds the format, re-ingests the affected nights, clears the flag
  and re-runs the weekly job. Nightly files are only merged by the weekly job,
  so re-ingesting a night means deleting that night's files and taking its
  batches off the ingested list. We expect this once or twice over the next few months.

### Operations

- **Notification:** email via `mail`, to a configurable list of recipients.
  It starts with olynn@andrew.cmu.edu; others can be added as wanted.
- **Safe re-runs:** after writing, the daily run recomputes row counts and
  max rows from the parquet footers on disk rather than adding to the old
  totals (unlike ppdb_hats). A run that crashes and is repeated can't
  double-count rows in the metadata.

### Accepted trade-offs (for now)

- **Pixel growth is acceptable for now.** The daily import never splits existing
  pixels. It only adds new pixels for alerts outside the current coverage.
  Pixels keep growing until the weekly re-import, and longer if a
  format-change flag makes it skip.
- **Stale margin and index are acceptable for now.** They are rebuilt only by
  the weekly re-import.

## Open questions

- **Source:** what is the new path of the live archive, and are alerts flowing yet?
- **Converter (deferred):** for now the pipeline starts from parquet chunks in
  the landing dir. Notes for when we port Neven's `import_alerts.py`:
  - It imports two helper modules, `probe` and `avro_to_arrow`, that we don't
    have yet. They contain the decoding and `build_canonical_schema`.
  - It builds tables with `pa.Table.from_pylist(records, schema=CANON)`, which
    silently drops keys missing from the fixed schema. That is probably why the dump has
    no 1101 fields. The Avro header's format ID is read during decoding but not kept.
  - It assumes new alerts always arrive in new raw subdirs and existing subdirs
    never change. Subdirs are alertId prefixes, not nights, so this should be checked.
  - It also strips `ssSource` and `mpc_orbits`, not just the cutouts.
- **Email delivery:** `mail` exists on the interactive nodes. Does it actually
  deliver when run from the nightly job? `mail` exiting cleanly only means it
  accepted the message, not that it was delivered, so the only real check is
  sending a test email from a batch job.

## Implementation plan

Small PRs, each reviewable in one sitting. PRs 2–4 don't depend on each
other or on the cluster. Nothing touches the shared filesystem until PR 6,
and nothing runs on a schedule until PR 9.

| # | PR | Contents | Size |
|---|---|---|---|
| 1 | **Design doc** | This document | no code |
| 2 | **Reader** | `reader.py` (the notebook's `AlertReader`), small test fixtures cut from the dump, tests | small |
| 3 | **Schema** | `schema.py`: 1101 target columns, casting, `schemaId` mock, unknown-format detection | small |
| 4 | **State + notify** | `state.py` (ingested log, format-change flag, lock), `notify.py` (email) | small |
| 5 | **Increment** | `daily/increment.py` from ppdb_hats with recompute-from-disk metadata; `config.py`, `pipeline.py` | medium |
| 6 | **Baseline** | `baseline.py`: full import of the Jun 26 chunks into `working/` | small |
| 7 | **Daily run + replay** | `daily/run.py`, `mock/replay.py`, end-to-end test (baseline → replay one night) | medium |
| 8 | **Weekly scaffold** | `weekly/run.py`: flag check, skip and email; merge itself deferred to lsdb#1536 | small |
| 9 | **Scheduling** | cron/Slurm scripts, run against the real v2 directory | small |
