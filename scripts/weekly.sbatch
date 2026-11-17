#!/bin/bash
# Re-import the current version of <root>/collections/ as a new version, with a margin
# and a diaSourceId index:
#
#   sbatch scripts/weekly.sbatch [root]
#
# To test without touching the real root, pass one holding copies of its collections/ and state/.
#
# Crontab line (run `crontab -e` on sdfcron001):
#
#   0 8 * * 6 cd $HOME/alert-archive/scripts && sbatch weekly.sbatch
#
# Runs once every Saturday at 08:00 Pacific, after the daily run.

#SBATCH --job-name=alert-archive-weekly
#SBATCH --account=rubin:default
#SBATCH --partition=milano
#SBATCH --nodes=1
#SBATCH --mem=0
#SBATCH --exclusive
#SBATCH --time=04:00:00
#SBATCH --output=%x.log
#SBATCH --open-mode=append
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=scampos@andrew.cmu.edu

umask 002 # Files written by the script have group write permissions

ROOT=${1:-/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_v2}

echo "=== $(date) job $SLURM_JOB_ID: $ROOT ==="

export SHELL=/bin/bash
export PYTHONUNBUFFERED=1
setup lsst_distrib

python -c "import sys; from alert_archive.weekly.run import main; print(main(sys.argv[1]))" "$ROOT"
