#!/bin/bash
#
# Copies the nightly results to the CMU machine behind alert-dashboard.lsdb.io.
#
# Crontab line (run `crontab -e` on sdfcron001):
#
#   0 5 * * * $HOME/alert-archive/benchmarks/nightly/rsync-bench.sh
#
# Runs at 05:00 Pacific, leaving time for the benchmarks submitted at 03:17 to finish.
set -uo pipefail

# The log file will be readable by everyone and writable by the group.
umask 0002

shared=/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_benchmarks
src=$shared/results/
dest=benchmarks@lsdb.phys.cmu.edu:/opt/services/alert-dashboard/lbench_results/

# Create the log next to this script, in our own clone of this repo.
log=$(dirname "$0")/rsync-bench.log
exec >> "$log" 2>&1

# Run rsync to the CMU server.
echo "$(date '+%F %T %Z') starting sync"
rsync_options=(
    --archive --compress
    --timeout=600
    --no-links
    --delay-updates
    "--chmod=D777,F666"  
    --rsh="ssh -o BatchMode=yes"
)
/usr/bin/rsync "${rsync_options[@]}" "$src" "$dest"
status=$?
echo "$(date '+%F %T %Z') finished with exit code $status"

# Copy the log to the shared logs/ directory.
set -C
tmp=$shared/logs/.rsync-bench.log.$$.$RANDOM
cat "$log" > "$tmp" && mv -f "$tmp" "$shared/logs/rsync-bench.log"

exit $status
