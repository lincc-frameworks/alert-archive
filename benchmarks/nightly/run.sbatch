#!/bin/bash
# Nightly alert archive benchmark job, submitted by submit.sh as:
#
#   sbatch run.sbatch <base dir> <LSST conda env name> <results dir>
#
# Runs on a whole milano node (RHEL 9.6) so timings stay comparable night to
# night. Preemptable: if preempted, the run is cancelled and that night has no
# results (sacct shows PREEMPTED).
#SBATCH --job-name=alert-archive-bench
#SBATCH --account=rubin:default
#SBATCH --partition=milano
#SBATCH --qos=preemptable
#SBATCH --constraint=OS_VER:9.6
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --time=02:00:00

set -o pipefail

# Make results and logs group-writable, so anyone in rubin_users can manage
# them (see the umask comment in submit.sh).
umask 0002

base=$1
repo=$base/alert-archive
results=$3

# Let git work in the shared clone even if another user created it (see the
# safe.directory comment in submit.sh).
export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory GIT_CONFIG_VALUE_0=$repo

# Pin the conda env rather than taking loadLSST.sh's default, which Rubin bumps.
# lsst_distrib isn't set up; the benchmarks only need the venv's packages.
export LSST_CONDA_ENV_NAME=$2
# The "shellcheck source=/dev/null" lines tell shellcheck (run on PRs by
# .github/workflows/nightly-benchmark-checks.yml) not to look inside the file
# sourced on the next line: these files only exist on S3DF, so the check would
# otherwise fail because it can't find them.
# shellcheck source=/dev/null
source /sdf/group/rubin/sw/loadLSST.sh
# Stop if the venv is missing or broken, rather than running the benchmarks
# with the LSST env's packages instead of the pinned ones.
# shellcheck source=/dev/null
source "$base/venv/bin/activate" || exit 1

# Ignore ~/.local (pip --user installs, e.g. an editable ~/lsdb). A venv made
# with --system-site-packages would otherwise still import from there.
export PYTHONNOUSERSITE=1

# Treat unset variables as errors from here on. Not set earlier because the
# conda activation scripts sourced above read variables that may be unset.
set -u

export LBENCH_ROOT=$results
export ALERT_ARCHIVE_CPROFILE=0

echo "node: $(hostname)  job: $SLURM_JOB_ID  alert-archive: $(git -C "$repo" rev-parse HEAD)"

cd "$repo" || exit 1
marker=$(mktemp)
python -m pytest --lbench benchmarks/test_alert_archive.py -p no:cacheprovider
status=$?

# Record what was benchmarked next to the results. pytest-benchmark already
# stores the node and the alert-archive commit in its JSON; this adds the
# exact package versions. Only look at run dirs created by this job, so a run
# that died before lbench made its dir can't overwrite an earlier night's file.
run_dir=$(find "$results" -mindepth 1 -maxdepth 1 -type d -newer "$marker" | sort | tail -1)
rm -f "$marker"
if [[ -z $run_dir ]]; then
    echo "no lbench run dir was created; pytest exit status $status" >&2
    exit $status
fi
{
    echo "slurm_job_id: $SLURM_JOB_ID"
    echo "node: $(hostname)"
    echo "lsst_conda_env: $LSST_CONDA_ENV_NAME"
    echo "alert_archive_commit: $(git -C "$repo" rev-parse HEAD)"
    echo "pytest_exit_status: $status"
    echo "--- pip freeze ---"
    pip freeze
} > "$run_dir/versions.txt"

exit $status
