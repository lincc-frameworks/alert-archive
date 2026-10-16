# Alert archive benchmarks

This directory holds timing benchmarks for common queries on the Rubin alert 
archive HATS catalog, run with 
[lf-bench](https://github.com/lincc-frameworks/lsdb-benchmarking) (`lbench`), a pytest plugin built on [pytest-benchmark](https://pytest-benchmark.readthedocs.io/). 

They run nightly on S3DF, and you can also run them by hand.

| File | What it is |
|---|---|
| `test_alert_archive.py` | The benchmarks (one test per query) |
| `conftest.py` | Test configuration (catalog openers, session-scoped Dask cluster, and overrides of lf-bench's fixtures) |
| `diaObjectId_samples.npy` | Object IDs sampled for `test_naive_lc_aggregation` |
| `nightly/submit.sh` | Nightly cron entry point: updates the job's clone, checks the venv, submits the Slurm job |
| `nightly/run.sbatch` | The Slurm job that runs the benchmarks |
| `nightly/requirements.txt` | Pinned lsdb, hats, lf-bench, … versions for nightly runs |

## Running benchmarks by hand

Run these from the repository root, in an environment with this repo's
dependencies installed (including lf-bench). The benchmarks read the catalogs
from `/sdf/data/rubin/...`, so they only run on S3DF.

### Running the full suite

```bash
cd ~/alert-archive
ALERT_ARCHIVE_CPROFILE=0 pytest --lbench benchmarks/
```

- `--lbench` turns on lf-bench: it creates a timestamped run folder, enables
  pytest-benchmark, and writes the results JSON there. Without it, every test
  fails with `lbench_run_dir not set — run pytest with --lbench option`.
- `ALERT_ARCHIVE_CPROFILE=0` skips the extra cProfile run of each test. Leave
  it out if you want the `.prof` files (see the diagram below).
- The full suite takes about 15–20 minutes. It's fine to run on an interactive
  node, but for anything you want to compare against nightly results, use the
  [branch run](#running-a-branch-the-way-the-nightly-job-does) below so it runs
  on the same hardware and pinned packages.

Early in the output you'll see:

```
[lbench] running benchmarks in .../lbench_results/<timestamp>
```

That folder holds your results. `lbench_results/` is gitignored.

### Running a single benchmark

`-k` selects tests whose names match an expression:

```bash
ALERT_ARCHIVE_CPROFILE=0 pytest --lbench benchmarks/ -k per_band
```

To list the test names first:

```bash
pytest benchmarks/ --collect-only -q
```

`test_naive_lc_aggregation` runs once per sample size (1, 100, 10,000). To
run one of those, give its full ID, in quotes because of the brackets:

```bash
ALERT_ARCHIVE_CPROFILE=0 pytest --lbench "benchmarks/test_alert_archive.py::test_naive_lc_aggregation[100]"
```

or combine names in `-k`, e.g. `-k "per_band or per_night"`.

### CLI Settings

| Variable | Default | Effect |
|---|---|---|
| `ALERT_ARCHIVE_CPROFILE` | `1` (on) | `0` skips the per-test cProfile run |
| `ALERT_ARCHIVE_DASK_WORKERS` | `2` | Number of Dask workers in the `LocalCluster` |
| `LBENCH_ROOT` | `./lbench_results` | Where lf-bench creates run folders |

Changing the worker count changes the timings, so results with different
worker counts aren't comparable.

lf-bench's `--lbench-root <dir>` option does the same as `LBENCH_ROOT`.

## Nightly runs

Every night at 03:17 Pacific, cron on `sdfcron001` (the only S3DF node where
user cron jobs are allowed) runs `nightly/submit.sh`, which:

1. resets the job's own clone of this repo to `origin/main`, so only merged
   work is benchmarked;
2. makes sure the job's venv matches `nightly/requirements.txt`, reinstalling
   when the pins change;
3. submits `nightly/run.sbatch` to Slurm.

The job runs on a whole `milano` node (RHEL 9.6) so timings are comparable
night to night. It uses the preemptable QOS: if a higher-priority job needs
the node, the run is cancelled and that night has no results.

If a run fails or hits its time limit, Slurm emails the job's owner (the
`--mail-user` address in `nightly/run.sbatch`). That covers branch runs too,
whoever starts them. Failures in `submit.sh` itself, before a job is
submitted, don't send email; they only show up in `logs/cron.log`.

Everything lives in a shared folder next to the alert archive itself:

```
/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_benchmarks/
```

Everyone in `rubin_users` can read and write it. The nightly scripts make
everything they create group-writable, so anyone in the group can run branch
tests here, clean up, or take over the job (see [Handover](#handover)). The
results are also copied nightly to another cluster that serves
visualizations of them.

```
rubin_alert_archive_benchmarks/
├── alert-archive/       the job's clone; reset on every run, don't edit it
├── venv/                pinned packages from nightly/requirements.txt
├── results/             nightly results from main; one folder per run
├── results-<branch>/    results from branch runs
└── logs/
    ├── cron.log         one line per step of each submit.sh run
    ├── pip.log          output of venv (re)installs
    └── <jobid>.out      output of each Slurm job
```

### Running a branch the way the nightly job does

To benchmark a branch on the nightly setup (same hardware, same pinned
packages), push it to GitHub, then on `sdfcron001`:

```bash
ssh sdfcron001
/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_benchmarks/alert-archive/benchmarks/nightly/submit.sh <branch>
```

Anyone in `rubin_users` can run this. The job uses the `rubin:default` Slurm
account, so you need access to it.

- **Check that no other run is in progress first**
  (`squeue -n alert-archive-bench`). All runs share one clone, and starting
  a new one resets it to your branch, under a run that's still using it.
- The branch must be pushed; the script checks out `origin/<branch>`.
- Results go to `results-<branch>/` (with `/` in the name replaced by `_`), so
  they stay separate from the nightly history.
- With no argument, `submit.sh` benchmarks `main`, the same as the cron job.
- If the branch changes `nightly/requirements.txt`, the shared venv is
  reinstalled with its pins. The next run from `main` puts `main`'s pins back.
- Changes to `submit.sh` itself take effect one run after they're pushed:
  the script reads its own code before it fetches the new version.

### Checking on a run

```bash
B=/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_benchmarks
tail $B/logs/cron.log                              # did submit.sh run, which commit, which job ID?
squeue -u $USER                                    # PD = waiting for a node, R = running
tail -f $B/logs/<jobid>.out                        # live output
sacct -j <jobid> -o JobID,State,Elapsed,MaxRSS     # COMPLETED / FAILED / PREEMPTED, time, memory
```

A `FAILED at line N` entry in `cron.log` means `submit.sh` itself failed
(e.g. a git or pip problem); check `pip.log` for install errors.

### Changing pinned versions

Edit `nightly/requirements.txt` and merge to `main`. The next nightly run
reinstalls the venv. Version changes then line up with commits in this repo,
so a jump in the results can be traced to them.

### Handover

The benchmark code lives in this repo, and the shared folder is writable by
everyone in `rubin_users`. The one thing tied to a person is the cron entry:
it lives in the owner's crontab on `sdfcron001`, and crontabs can't be
shared. Current owner: Olivia Lynn (`olynn`).

To take over:

1. **Stop the old cron entry.** The previous owner runs `crontab -e` on
   `sdfcron001` and deletes the line. Otherwise both entries run and the job
   runs twice a night. If the previous owner's account is gone, so is their
   crontab.
2. **Add your own cron entry** with `crontab -e` on `sdfcron001`:
   ```
   17 3 * * * /sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_benchmarks/alert-archive/benchmarks/nightly/submit.sh
   ```
3. **Check the next morning** that it ran (see
   [Checking on a run](#checking-on-a-run)), or run `submit.sh` once by hand
   on `sdfcron001` to check right away.
4. **Update the repo** in a PR: (1) the owner named in this section, and (2) the
   `--mail-user` address at the top of `nightly/run.sbatch`, which gets an
   email from Slurm whenever a run fails or hits its time limit.

If the shared folder is ever lost, `submit.sh` can rebuild everything except
the results: run it with `ALERT_ARCHIVE_NIGHTLY_BASE` set to a new, empty
folder, and it clones the repo, builds the venv, and submits a run.

## Reading results

Each run folder contains:

- `pytest-benchmark.json`: per-test timing stats, plus Dask metrics under
  `extra_info.dask` (`peak_memory_bytes`, `n_tasks`, path to the performance
  report). pytest-benchmark also records the node and the git commit.
- `dask_performance_report_*.html`: one per test; open it in a browser to see
  the task stream and worker memory.
- `versions.txt` (nightly and branch runs only): Slurm job ID, node, LSST
  env, alert-archive commit, and `pip freeze` of the venv.

In the summary table pytest prints:

- Numbers in parentheses are ratios to the best value in that column.
- **Outliers** `A;B`: A is the number of rounds more than 1 standard
  deviation from the mean (1–2 of 5 is normal); B is the number outside
  1.5 × IQR, the standard box-plot outlier rule. A single B outlier in one
  run usually means a brief hiccup, e.g. on the shared filesystem.
- **Rounds** should be 5 (see below).

## What one benchmark runs

Each test builds a query and passes its `.compute` to the `lbench_dask`
fixture. `conftest.py` overrides lf-bench's `lbench` fixture, so one test
runs the query like this:

```
test_xxx(alert_archive, lbench_dask)
│
└─ lbench_dask(func)                              ← lf-bench
   │
   ├─ lbench(func)                                ← conftest.py override
   │   │
   │   ├─ benchmark.pedantic(func)                ← pytest-benchmark
   │   │   ├─ warm-up round   func()  ×1   untimed; warms caches
   │   │   └─ timed rounds    func()  ×5   these are the reported stats
   │   │
   │   ├─ memray     func()  ×1   only for tests marked lbench_memory (none are)
   │   └─ cProfile   func()  ×1   skipped when ALERT_ARCHIVE_CPROFILE=0
   │
   └─ run_dask_benchmark(func)    ×1   Dask task stream, peak worker memory,
                                       performance report
                                   ───
                             total  7 full .compute() calls per test
                                    (8 with cProfile on)
```

Things this means for interpreting results:

- **Timings are warm-cache.** The warm-up round reads the data from disk
  first, so the timed rounds mostly read from the OS page cache. That's
  deliberate, but it means the numbers describe repeated queries, not a
  first query on a cold node.
- **The Dask metrics come from a separate run**, not from the timed rounds.
- **The cProfile run only profiles the driver process.** The real work runs
  in the Dask workers, so it shows mostly waiting. That's why it's off in
  nightly runs.
- **What's inside the timer:** every test opens the catalog *before* calling
  `lbench_dask`, so opening it isn't timed. Most tests also build the query
  beforehand and time only `.compute()`; `test_per_night_counts` and
  `test_per_band_counts` time a function that also builds the query and sums
  the per-partition counts.
- **The Dask cluster is shared across tests** in a session, so a test can
  benefit from data an earlier test already read.

## Known issues

- A run that's interrupted (Ctrl-C, preemption, timeout) leaves a run folder
  with no `pytest-benchmark.json`, because results are only written at the
  end. That's expected; check `sacct` or the job's `.out` file to see what
  happened.
