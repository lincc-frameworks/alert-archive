"""Tests for the lf-bench fixture overrides in benchmarks/conftest.py.

The benchmarks themselves need the S3DF catalogs, but the overrides don't.
Most tests here run a small pytest session (via pytest's ``pytester``) with
the real benchmarks/conftest.py and ``--lbench``, benchmarking a tiny Dask
computation that counts how often it's called.
"""

import json
import site
from pathlib import Path

import pytest

pytest.importorskip("lbench", reason="needs the benchmark extra: pip install -e '.[benchmark]'")

pytest_plugins = ["pytester"]

BENCHMARKS_CONFTEST = Path(__file__).parents[2] / "benchmarks" / "conftest.py"


@pytest.fixture(autouse=True)
def keep_user_site(monkeypatch):
    """Let inner pytest sessions see packages installed with ``pip install --user``.

    pytester points HOME at a temporary directory, which moves Python's user
    site-packages (~/.local) out of reach. Pin it to this session's location.
    """
    monkeypatch.setenv("PYTHONUSERBASE", site.getuserbase())


# Stand-in for a benchmark: a small Dask computation that counts its calls and
# writes the count to calls.txt once lbench_dask is done with it.
COUNTING_BENCHMARK = """
from pathlib import Path

import dask.array as da


def test_counted(lbench_dask):
    calls = []

    def query():
        calls.append(1)
        return da.ones(100, chunks=10).sum().compute()

    lbench_dask(query)
    Path("calls.txt").write_text(str(len(calls)))
"""


def run_lbench(pytester, monkeypatch, *, cprofile):
    """Run COUNTING_BENCHMARK under --lbench; return the results JSON path and the call count."""
    pytester.makeconftest(BENCHMARKS_CONFTEST.read_text())
    pytester.makepyfile(test_bench=COUNTING_BENCHMARK)
    results_root = pytester.path / "results"
    monkeypatch.setenv("LBENCH_ROOT", str(results_root))
    monkeypatch.setenv("ALERT_ARCHIVE_CPROFILE", "1" if cprofile else "0")

    # A subprocess keeps the inner session's plugins, Dask cluster and
    # environment separate from this one.
    result = pytester.runpytest_subprocess("--lbench", "-p", "no:cacheprovider")
    result.assert_outcomes(passed=1)

    (run_dir,) = results_root.iterdir()
    calls = int((pytester.path / "calls.txt").read_text())
    return run_dir / "pytest-benchmark.json", calls


def test_results_json_is_written(pytester, monkeypatch):
    """``--lbench`` writes a non-empty, valid pytest-benchmark.json.

    Older lf-bench left this file empty with pytest-benchmark >= 5.3.0
    (fixed in lsdb-benchmarking#20); this guards against that coming back.
    """
    json_path, _ = run_lbench(pytester, monkeypatch, cprofile=False)

    assert json_path.stat().st_size > 0
    benchmarks = json.loads(json_path.read_text())["benchmarks"]
    assert len(benchmarks) == 1


@pytest.mark.parametrize(
    ("cprofile", "expected_calls"),
    [
        # 1 warm-up + 5 timed rounds + 1 Dask metrics run
        (False, 7),
        # ... + 1 cProfile run
        (True, 8),
    ],
)
def test_call_counts(pytester, monkeypatch, cprofile, expected_calls):
    """Each benchmark calls its function the number of times benchmarks/README.md documents.

    With cProfile off: 1 untimed warm-up, 5 timed rounds, and 1 Dask metrics
    run (7 calls). With cProfile on, 1 more (8).
    """
    json_path, calls = run_lbench(pytester, monkeypatch, cprofile=cprofile)

    assert calls == expected_calls
    (benchmark,) = json.loads(json_path.read_text())["benchmarks"]
    assert benchmark["stats"]["rounds"] == 5
    assert ("cprofile_path" in benchmark["extra_info"]) == cprofile


def test_dask_metrics_are_recorded(pytester, monkeypatch):
    """The Dask metrics run records task count, peak worker memory, and a performance report."""
    json_path, _ = run_lbench(pytester, monkeypatch, cprofile=False)

    (benchmark,) = json.loads(json_path.read_text())["benchmarks"]
    dask_info = benchmark["extra_info"]["dask"]
    assert dask_info["n_tasks"] > 0
    assert dask_info["peak_memory_bytes"] > 0
    assert Path(dask_info["performance_report"]).is_file()


@pytest.mark.parametrize(("workers_env", "expected_workers"), [(None, 2), ("3", 3)])
def test_dask_worker_count(pytester, monkeypatch, workers_env, expected_workers):
    """The benchmarks' Dask cluster has single-threaded workers: 2, or ALERT_ARCHIVE_DASK_WORKERS."""
    pytester.makeconftest(BENCHMARKS_CONFTEST.read_text())
    pytester.makepyfile(
        test_workers="""
        from pathlib import Path

        def test_workers(single_thread_dask_client):
            nthreads = single_thread_dask_client.nthreads()
            Path("workers.txt").write_text(f"{len(nthreads)} {max(nthreads.values())}")
        """
    )
    if workers_env is None:
        monkeypatch.delenv("ALERT_ARCHIVE_DASK_WORKERS", raising=False)
    else:
        monkeypatch.setenv("ALERT_ARCHIVE_DASK_WORKERS", workers_env)

    result = pytester.runpytest_subprocess("-p", "no:cacheprovider")
    result.assert_outcomes(passed=1)

    n_workers, max_threads = map(int, (pytester.path / "workers.txt").read_text().split())
    assert n_workers == expected_workers
    assert max_threads == 1
