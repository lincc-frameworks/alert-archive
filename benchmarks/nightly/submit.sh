#!/bin/bash
# Entry point for the nightly alert archive benchmarks.
#
# Usage (on sdfcron001, the only S3DF node where user cron jobs are allowed):
#
#   submit.sh            # benchmark origin/main; what cron runs every night
#   submit.sh <branch>   # benchmark origin/<branch> instead, for testing
#
# Crontab line (run `crontab -e` on sdfcron001; <you> is your username):
#
#   17 3 * * * /sdf/data/rubin/user/<you>/rubin_alert_archive_benchmarks/alert-archive/benchmarks/nightly/submit.sh
#
# Runs at 03:17 Pacific: overnight, when the cluster is quietest and preemption
# is least likely, and off the hour, when many other cron jobs fire.
#
# What it does:
#   1. Resets the job's own clone of alert-archive to the requested branch on
#      GitHub (main by default), so only pushed & committed work is benchmarked;
#      never anyone's local checkout. Clones it first if it doesn't exist yet.
#      This happens here, not in the Slurm job, because compute nodes have
#      limited outbound network access.
#   2. Makes sure the venv with the pinned packages exists and matches
#      benchmarks/nightly/requirements.txt, reinstalling when the pins change.
#   3. Submits run.sbatch, which runs the benchmarks on a compute node.
#
# Files live in two places, split by who may write them:
#
#   $base (private, per person; only its owner can write):
#     alert-archive/      the job's clone; reset on every run, never edit by hand
#     venv/               pinned lsdb, hats, lf-bench, ... (see requirements.txt)
#     pip.log             output of venv (re)installs
#
#   $shared (writable by everyone in rubin_users):
#     results/            nightly results from main; one lbench run dir per night
#     results-<branch>/   results from test runs of other branches, kept apart so
#                         they don't mix into the nightly history
#     logs/cron.log       one line per step of every submit.sh run, by anyone
#     logs/<jobid>.out    output of each Slurm job
#
# The clone and venv are code that runs as whoever runs this script, so they
# must not be writable by anyone else: otherwise another user could edit them
# and have their code run under your account. Only results and logs, which are
# data, are shared.
#
# This script lives inside the clone that step 1 resets. Bash reads a script
# from disk as it runs it, so if git changed this file mid-run, bash could run
# a mix of old and new lines. Putting everything in main() and calling it on
# the last line makes bash read the whole body before running any of it.

main() {
    # Stop at the first failing command (-e), treat unset variables as errors
    # (-u), and count a pipeline as failed if any part of it fails (pipefail).
    set -euo pipefail

    # Files this script creates are writable only by their owner (the clone
    # and venv in particular). Writes to the shared folder below opt in to
    # group write explicitly.
    umask 0022

    local user
    user=$(id -un)

    # Private folder for the clone and venv. Defaults to one per person, so
    # everyone who runs this (the nightly owner, or anyone testing a branch)
    # gets their own. Override with ALERT_ARCHIVE_NIGHTLY_BASE.
    local base=${ALERT_ARCHIVE_NIGHTLY_BASE:-/sdf/data/rubin/user/$user/rubin_alert_archive_benchmarks}

    # Shared folder for results and logs (readable and writable by
    # rubin_users). Override with ALERT_ARCHIVE_NIGHTLY_SHARED, e.g. to try
    # the setup without writing to the real results.
    local shared=${ALERT_ARCHIVE_NIGHTLY_SHARED:-/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_benchmarks}

    # LSST conda env the venv is layered on. Pinned (rather than whatever
    # loadLSST.sh defaults to) so a Rubin-side upgrade can't silently change
    # the environment between nights. Changing it means recreating the venv:
    # delete $base/venv and the next run rebuilds it.
    local lsst_env=lsst-scipipe-14.0.0

    # The branch to benchmark: first argument, or main when none is given (cron).
    local branch=${1:-main}

    local repo=$base/alert-archive
    local venv=$base/venv
    local logs=$shared/logs

    # main's results go in results/; any other branch gets its own folder.
    # Slashes in branch names (e.g. user/feature) become underscores so the
    # name works as a single directory name.
    local results=$shared/results
    if [[ $branch != main ]]; then
        results=$shared/results-${branch//\//_}
    fi

    mkdir -p "$base"
    # In the shared folder, create folders and log files group-writable
    # (umask 0002, in a subshell so it doesn't apply to anything else), so
    # teammates can clean up and their own runs can append to cron.log.
    (umask 0002 && mkdir -p "$logs" "$results")

    # Append a timestamped line to cron.log, tagged with who ran it and which
    # branch. Cron doesn't show output anywhere by default, so this log is how
    # to see what happened on a given night.
    log() { (umask 0002 && echo "$(date -Is) [$user $branch] $*" >> "$logs/cron.log"); }

    # If any command fails (and set -e is about to stop the script), record
    # where, so a missing night's results can be traced back to this step.
    trap 'log "FAILED at line $LINENO; see $logs/cron.log and $base/pip.log"' ERR

    # --- 1. Update the clone ------------------------------------------------
    # A new base folder has no clone yet, so make one first. This lets a fresh
    # setup start by running submit.sh from any checkout of the repo.
    if [[ ! -d $repo/.git ]]; then
        log "cloning alert-archive into $repo"
        git clone --quiet https://github.com/lincc-frameworks/alert-archive.git "$repo"
    fi
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
        "$venv/bin/pip" install -r "$reqs" >> "$base/pip.log" 2>&1
        # Only written after pip succeeds, so a failed install is retried on
        # the next run instead of being treated as done.
        cp "$reqs" "$stamp"
    fi
    # Note: testing a branch with different pins reinstalls your venv with
    # that branch's pins. The next run from main sees that the file differs
    # and reinstalls main's pins, so nightly results stay on them.

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
