#!/bin/bash
# Re-import the catalog as a new version of <root>/collections/, with a margin and a
# diaSourceId index. The first run reads <root>/working/:
#
#   sbatch scripts/weekly.sbatch [root]
#
# To test without touching the real root, pass one holding copies of its working/ and state/.
#
#SBATCH --job-name=alert-archive-weekly
#SBATCH --account=rubin:default
#SBATCH --partition=milano
#SBATCH --nodes=1
#SBATCH --mem=0
#SBATCH --exclusive
#SBATCH --time=04:00:00
#SBATCH --output=%x.log
#SBATCH --open-mode=append
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=scampos@andrew.cmu.edu

umask 002 # Files written by the script have group write permissions

ROOT=${1:-/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_v2}

echo "=== $(date) job $SLURM_JOB_ID: $ROOT ==="

export SHELL=/bin/bash
export PYTHONUNBUFFERED=1
setup lsst_distrib

python -c "import sys; from alert_archive.weekly.run import main; print(main(sys.argv[1]))" "$ROOT"
