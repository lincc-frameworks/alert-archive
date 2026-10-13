#!/bin/bash
# Entry point for the nightly alert archive benchmarks.
#
# Usage (on sdfcron001, the only S3DF node where user cron jobs are allowed):
#
#   submit.sh            # benchmark origin/main; what cron runs every night
#   submit.sh <branch>   # benchmark origin/<branch> instead, for testing
#
# Crontab line (run `crontab -e` on sdfcron001):
#
#   17 3 * * * /sdf/data/rubin/user/olynn/alert_archive_benchmarks/alert-archive/benchmarks/nightly/submit.sh
#
# Runs at 03:17 Pacific: overnight, when the cluster is quietest and preemption
# is least likely, and off the hour, when many other cron jobs fire.
#
# What it does:
#   1. Resets the job's own clone of alert-archive to the requested branch on
#      GitHub (main by default), so only pushed & committed work is benchmarked;
#      never anyone's local checkout. This happens here, not in the Slurm job,
#      because compute nodes have limited outbound network access.
#   2. Makes sure the venv with the pinned packages exists and matches
#      benchmarks/nightly/requirements.txt, reinstalling when the pins change.
#   3. Submits run.sbatch, which runs the benchmarks on a compute node.
#
# Layout under $base:
#   alert-archive/      the job's clone; reset on every run, never edit by hand
#   venv/               pinned lsdb, hats, lf-bench, ... (see requirements.txt)
#   results/            nightly results from main; one lbench run dir per night
#   results-<branch>/   results from test runs of other branches, kept apart so
#                       they don't mix into the nightly history
#   logs/cron.log       one line per step of every submit.sh run
#   logs/pip.log        output of venv (re)installs
#   logs/<jobid>.out    output of each Slurm job
#
# This script lives inside the clone that step 1 resets. Bash reads a script
# from disk as it runs it, so if git changed this file mid-run, bash could run
# a mix of old and new lines. Putting everything in main() and calling it on
# the last line makes bash read the whole body before running any of it.

main() {
    # Stop at the first failing command (-e), treat unset variables as errors
    # (-u), and count a pipeline as failed if any part of it fails (pipefail).
    set -euo pipefail

    # Root of everything the nightly job owns. Override with
    # ALERT_ARCHIVE_NIGHTLY_BASE, e.g. to try the setup in a scratch location.
    local base=${ALERT_ARCHIVE_NIGHTLY_BASE:-/sdf/data/rubin/user/olynn/alert_archive_benchmarks}

    # LSST conda env the venv is layered on. Pinned (rather than whatever
    # loadLSST.sh defaults to) so a Rubin-side upgrade can't silently change
    # the environment between nights. Changing it means recreating the venv:
    # delete $base/venv and the next run rebuilds it.
    local lsst_env=lsst-scipipe-14.0.0

    # The branch to benchmark: first argument, or main when none is given (cron).
    local branch=${1:-main}

    local repo=$base/alert-archive
    local venv=$base/venv
    local logs=$base/logs

    # main's results go in results/; any other branch gets its own folder.
    # Slashes in branch names (e.g. user/feature) become underscores so the
    # name works as a single directory name.
    local results=$base/results
    if [[ $branch != main ]]; then
        results=$base/results-${branch//\//_}
    fi
    mkdir -p "$logs" "$results"

    # Append a timestamped line to cron.log. Cron doesn't show output anywhere
    # by default, so this log is how to see what happened on a given night.
    log() { echo "$(date -Is) [$branch] $*" >> "$logs/cron.log"; }

    # If any command fails (and set -e is about to stop the script), record
    # where, so a missing night's results can be traced back to this step.
    trap 'log "FAILED at line $LINENO; see $logs/cron.log and $logs/pip.log"' ERR

    # --- 1. Update the clone ------------------------------------------------
    # fetch: download the latest commits from GitHub without changing files.
    # checkout --force --detach origin/<branch>: make the files match that
    # branch exactly, throwing away any local edits (--force), without creating
    # a local branch that would need keeping in sync (--detach).
    git -C "$repo" fetch --quiet origin
    git -C "$repo" checkout --quiet --force --detach "origin/$branch"
    log "alert-archive at $(git -C "$repo" rev-parse --short HEAD)"

    # --- 2. Make sure the venv matches the pins -----------------------------
    # The venv sees the LSST env's packages (--system-site-packages), so dask,
    # astropy, etc. come from there; only the pinned packages are installed
    # into it.
    #
    # A venv made with --system-site-packages still sees ~/.local (the "user
    # site", where `pip install --user` puts things). PYTHONNOUSERSITE=1 turns
    # that off, so personal installs (e.g. the editable ~/lsdb) can't leak in.
    # pip needs it too: otherwise it sees a package in ~/.local, reports
    # "already satisfied", and skips installing it into the venv.
    # run.sbatch sets the same variable for the benchmarks themselves.
    export PYTHONNOUSERSITE=1
    local reqs=$repo/benchmarks/nightly/requirements.txt
    # Copy of the requirements file as of the last successful install. If the
    # current file differs, the pins changed and the venv needs updating.
    local stamp=$venv/installed-requirements.txt

    if [[ ! -x $venv/bin/python ]]; then
        log "creating venv at $venv"
        "/sdf/group/rubin/sw/conda/envs/$lsst_env/bin/python" -m venv --system-site-packages "$venv"
    fi
    # cmp -s compares silently and fails when the files differ, or when the
    # stamp doesn't exist yet (first run).
    if ! cmp -s "$reqs" "$stamp"; then
        log "requirements changed; reinstalling venv packages"
        "$venv/bin/pip" install -r "$reqs" >> "$logs/pip.log" 2>&1
        # Only written after pip succeeds, so a failed install is retried on
        # the next run instead of being treated as done.
        cp "$reqs" "$stamp"
    fi
    # Note: testing a branch with different pins reinstalls the shared venv
    # with that branch's pins. The next run from main sees that the file
    # differs and reinstalls main's pins, so nightly results stay on them.

    # --- 3. Submit the job --------------------------------------------------
    # --parsable makes sbatch print just the job ID, for the log.
    # --output: Slurm writes the job's stdout/stderr to logs/<jobid>.out
    # (%j is replaced by the job ID).
    # Arguments to run.sbatch: base dir, LSST env name, results dir.
    local job_id
    job_id=$(sbatch --parsable --output="$logs/%j.out" \
        "$repo/benchmarks/nightly/run.sbatch" "$base" "$lsst_env" "$results")
    log "submitted job $job_id; results will go to $results"
}

main "$@"
# Exit right after main returns, so bash never reads past this point in a file
# that git may have rewritten.
exit
