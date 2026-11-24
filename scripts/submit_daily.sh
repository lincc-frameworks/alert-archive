#!/bin/bash
# Cron entry point for the daily run: updates a dedicated clone of alert-archive,
# then submits scripts/daily.sbatch from it.
#
# Usage (on sdfcron001, the only S3DF node where user cron jobs are allowed):
#
#   submit_daily.sh            # run origin/main; what cron runs every night
#   submit_daily.sh <branch>   # run origin/<branch> instead. This writes the
#                              # real catalog, like any daily run.
#
# Crontab line (run `crontab -e` on sdfcron001; <you> is your username):
#
#   7 5 * * * /sdf/data/rubin/user/<you>/rubin_alert_archive_daily/alert-archive/scripts/submit_daily.sh
#
# Runs at 05:07 Pacific, between the converter (04:00, usually done in
# seconds) and the nightly benchmarks (06:17, which skip the night if the
# daily run still holds the lock). Off the hour, when many other cron jobs
# fire. See "Schedule" in scripts/README.md.
#
# What it does:
#   1. Skips the night if a daily job of yours is still queued or running.
#   2. Resets the job's own clone to origin/<branch> (main by default), so
#      only pushed, committed code builds the catalog; never anyone's local
#      checkout. Clones it first if it doesn't exist yet. This happens here,
#      not in the Slurm job, because compute nodes have limited network access.
#   3. Submits daily.sbatch from the clone, with PYTHONPATH pointing at the
#      clone's src/, so alert_archive imports from it rather than from an
#      editable install elsewhere (e.g. ~/.local pointing at ~/alert-archive).
#
# Files:
#   $base (private, per person; only its owner can write):
#     alert-archive/        the job's clone; reset on every run, never edit by hand
#   $logs (in the archive root, writable by rubin_users):
#     cron.log              one line per step of every run of this script
#     alert-archive-daily-<jobid>.log   output of each Slurm job
#
# The clone is code that runs under your account, so it must not be writable
# by anyone else. Only the logs are shared.
#
# This script lives inside the clone that step 2 resets. Bash reads a script
# from disk as it runs it, so everything is in main(), which bash reads whole
# before running any of it.

main() {
    # Cron starts a plain, non-login shell, without the `module` command or
    # S3DF's modulefile directory, so load Slurm (sbatch, squeue) by hand.
    # Done before `set -u`, as these site scripts read unset variables.
    # shellcheck source=/dev/null
    source /etc/profile.d/modules.sh
    # shellcheck source=/dev/null
    source /etc/profile.d/01-modulepath.sh
    module load slurm

    set -euo pipefail

    # Files this script creates are writable only by their owner (the clone in
    # particular). Writes to the shared logs opt in to group write explicitly.
    umask 0022

    local user
    user=$(id -un)

    local base=${ALERT_ARCHIVE_DAILY_BASE:-/sdf/data/rubin/user/$user/rubin_alert_archive_daily}
    local logs=${ALERT_ARCHIVE_DAILY_LOGS:-/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_v2/logs}
    local branch=${1:-main}
    local repo=$base/alert-archive

    mkdir -p "$base"
    (umask 0002 && mkdir -p "$logs" && touch "$logs/cron.log")

    # Cron shows output nowhere by default, so log each step, and copy error
    # messages (git, sbatch, ...) into cron.log too.
    log() { (umask 0002 && echo "$(date -Is) [$user $branch] $*" >> "$logs/cron.log"); }
    exec 2> >(tee -a "$logs/cron.log" >&2)
    trap 'log "FAILED at line $LINENO"' ERR

    # --- 1. Skip if the last run hasn't finished ----------------------------
    # A second job would only fail on the pipeline lock (and email about it).
    local queued
    queued=$(squeue --me --noheader --name=alert-archive-daily --format=%i | paste -sd, -)
    if [[ -n $queued ]]; then
        log "skipped: daily job(s) $queued still queued or running"
        exit 0
    fi

    # --- 2. Update the clone ------------------------------------------------
    if [[ ! -d $repo/.git ]]; then
        log "cloning alert-archive into $repo"
        git clone --quiet https://github.com/lincc-frameworks/alert-archive.git "$repo"
    fi
    git -C "$repo" fetch --quiet origin
    git -C "$repo" checkout --quiet --force --detach "origin/$branch"
    local commit
    commit=$(git -C "$repo" rev-parse --short HEAD)
    log "alert-archive at $commit"

    # alert_archive imports its version from _version.py, which `pip install`
    # normally generates (setuptools_scm). The clone isn't installed, so write
    # it here. It's gitignored, so daily.sbatch's uncommitted-changes check
    # doesn't see it.
    printf '__version__ = version = "0+g%s"\n' "$commit" > "$repo/src/alert_archive/_version.py"

    # --- 3. Submit the job --------------------------------------------------
    # daily.sbatch treats the directory sbatch runs in as the repo, so submit
    # from the clone. --parsable prints just the job ID, for the log.
    local job_id
    job_id=$(cd "$repo" && sbatch --parsable \
        --output="$logs/alert-archive-daily-%j.log" \
        --export="ALL,PYTHONPATH=$repo/src" \
        scripts/daily.sbatch)
    log "submitted job $job_id; log in $logs/alert-archive-daily-$job_id.log"
}

main "$@"
# Exit right after main returns, so bash never reads past this point in a file
# that git may have rewritten.
exit
