#!/bin/bash
# Replay the parquet dump's chunks after the baseline through the daily run,
# one alertId prefix (about one night) per run. See "The replay" in
# docs/design/incremental_data_flow.md.
#
# Run it from the root of a clone whose editable install
# (pip install -e .) points at that clone, with no uncommitted changes:
#
#   sbatch --mail-user=<you@example.org> scripts/replay.sbatch
#
# To replay only the first N nights, e.g. as a trial:
#
#   sbatch --mail-user=<you@example.org> --export=ALL,MAX_NIGHTS=1 scripts/replay.sbatch
#
# The log, alert-archive-replay-<jobid>.log, lands in the repo root.
# The replay can be stopped and resubmitted: nights already ingested are skipped.
# If the job is killed mid-run, it leaves state/lock behind; check that nothing
# is running, delete the lock, then resubmit. Files from the interrupted run
# are cleaned up automatically.
# It needs the baseline (scripts/baseline.sbatch) first.
#
# Uses the LSST conda env named by ALERT_ARCHIVE_LSST_ENV (default
# lsst-scipipe-13.0.0), with whatever lsdb/hats/hats-import it sees,
# including packages in ~/.local.
#SBATCH --job-name=alert-archive-replay
#SBATCH --account=rubin:default
#SBATCH --partition=milano
#SBATCH --constraint=OS_VER:9.6
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --time=08:00:00
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --output=%x-%j.log

set -eo pipefail
export SHELL=/bin/bash  # loadLSST.sh rejects the /bin/sh that a cron-launched sbatch passes on
export LSST_CONDA_ENV_NAME="${ALERT_ARCHIVE_LSST_ENV:-lsst-scipipe-13.0.0}"
source /sdf/group/rubin/sw/loadLSST.sh

# Treat unset variables as errors from here on. Not set earlier because the conda
# activation scripts sourced above read variables that may be unset, e.g. when
# sbatch passes on an env that was active in the submitting shell.
set -u

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

# Default Dask settings (8 workers x 16 GiB), temp files under $LSCRATCH (node-local).
# TEMPORARY: landing/ isn't group-writable yet, so the replay copies into (and the
# daily runs read from) our own replay_landing/. Remove before the PR for step 7.
python -c "
from alert_archive.config import PathConfig
PathConfig.landing_dir = property(lambda self: self.root / 'replay_landing')
from alert_archive.mock.replay import main
print(main(max_nights=${MAX_NIGHTS:-None}))
"
