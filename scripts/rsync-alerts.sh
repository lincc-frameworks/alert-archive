#!/bin/bash
#
# Copies the nightly results to UW (via arnor).
#
# Crontab line (run `crontab -e` on sdfcron001):
#
#   17 6 * * * $HOME/alert-archive/scripts/rsync-alerts.sh
#
# Runs at 06:17 Pacific, after the daily pipeline runs.
set -uo pipefail

# Requires an SSH connection to arnor.astro.washington.edu.
src=/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_v2/working/
dest=arnor:/astro/store/shire/hats/catalogs/rubin_alert_archive/

# Create the log next to this script, in our own clone of this repo.
log=$(dirname "$0")/rsync-alerts.log
exec >> "$log" 2>&1

echo "$(date '+%F %T %Z') starting sync"
rsync_options=(
    --archive --compress
    --timeout=600
    --no-links
    --delay-updates
    --chown=:lsst
    "--chmod=D775,F664"
    --rsh="ssh -o BatchMode=yes"
    --delete
)
/usr/bin/rsync "${rsync_options[@]}" "$src" "$dest"
status=$?
echo "$(date '+%F %T %Z') finished with exit code $status"

exit $status