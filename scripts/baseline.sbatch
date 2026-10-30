#!/bin/bash
# Build the working catalog (rubin_alert_archive_v2/working) from the parquet
# dump's Jun 26 chunks. See "The baseline" in docs/design/incremental_data_flow.md.
#
# Run it once, from the root of a clone whose editable install
# (pip install -e .) points at that clone, with no uncommitted changes:
#
#   sbatch --mail-user=<you@example.org> scripts/baseline.sbatch
#
# The log, alert-archive-baseline-<jobid>.log, lands in the repo root
# (*.log is gitignored). It starts with the branch and commit that ran.
#
# The baseline refuses to run if working/ or state/ingested.jsonl already
# exists. To rebuild, delete both first.
#
# Not preemptable on purpose: a cancelled import leaves a partial working/,
# which then has to be deleted by hand.
#
# Uses the LSST conda env named by ALERT_ARCHIVE_LSST_ENV (default
# lsst-scipipe-13.0.0), with whatever lsdb/hats/hats-import it sees,
# including packages in ~/.local.
#SBATCH --job-name=alert-archive-baseline
#SBATCH --account=rubin:default
#SBATCH --partition=milano
#SBATCH --constraint=OS_VER:9.6
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --time=04:00:00
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --output=%x-%j.log

set -euo pipefail
export SHELL=/bin/bash  # loadLSST.sh rejects the /bin/sh that a cron-launched sbatch passes on
export LSST_CONDA_ENV_NAME="${ALERT_ARCHIVE_LSST_ENV:-lsst-scipipe-13.0.0}"
source /sdf/group/rubin/sw/loadLSST.sh

repo="${SLURM_SUBMIT_DIR:?submit this with sbatch from the repo root}"
cd "$repo"

# The run must use this clone's committed code, so the log says exactly what built the catalog.
if ! git diff --quiet HEAD; then
    echo "$repo has uncommitted changes; commit or stash them first." >&2
    git status --short >&2
    exit 1
fi
package=$(python -c "import alert_archive, pathlib; print(pathlib.Path(alert_archive.__file__).resolve().parent)")
if [ "$package" != "$(realpath "$repo/src/alert_archive")" ]; then
    echo "alert_archive imports from $package, not $repo/src; pip install -e $repo first." >&2
    exit 1
fi
echo "Branch: $(git rev-parse --abbrev-ref HEAD)"
echo "Commit: $(git log -1 --oneline)"
echo "LSST env: $LSST_CONDA_ENV_NAME"

umask 0002  # group-writable, so other rubin_users can read and later jobs can write

# Defaults: 8 Dask workers x 16 GiB, temp files under $LSCRATCH (node-local).
python -c "from alert_archive.baseline import main; print(main())"
