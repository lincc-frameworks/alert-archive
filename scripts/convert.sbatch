#!/bin/bash
# Convert the raw-archive subdirectories that are new since the last run:
#
#   sbatch scripts/convert.sbatch [out-dir]
#
#SBATCH --job-name=convert-from-avro
#SBATCH --account=rubin:default
#SBATCH --partition=milano
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --time=01:00:00
#SBATCH --output=/sdf/home/s/stavar/alert-archive/scripts/%x.log
#SBATCH --open-mode=append
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=scampos@andrew.cmu.edu

OUT_DIR=${1:-/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_v2/landing}

umask 002

echo "=== $(date) job $SLURM_JOB_ID: $OUT_DIR ==="

export SHELL=/bin/bash
export PYTHONUNBUFFERED=1

source /sdf/group/rubin/sw/loadLSST.sh
setup lsst_distrib
python -m alert_archive convert-from-avro --out-dir "$OUT_DIR"
