#!/bin/bash
# ============================================================
# Flood Pipeline — Stage 09 ML Classification (SLURM array)
#
# Splits 269 floods into 14 batches of 20 floods each.
# Each job loads ~1k rows + 2GB model → well under 16GB.
# PyArrow filter pushdown ensures only the batch rows are read.
#
# Submit all 14 jobs with one command:
#   sbatch run_stage09_split.sh
#
# Monitor:
#   squeue -u $USER
#
# After all jobs COMPLETED, merge outputs:
#   python3 scripts/merge_stage09_splits.py
# ============================================================

#SBATCH --job-name=s09_split
#SBATCH --output=/home/scur0742/Flood-News-Data-Pipeline/logs/s09_split_%A_%a.out
#SBATCH --error=/home/scur0742/Flood-News-Data-Pipeline/logs/s09_split_%A_%a.err
#SBATCH --array=0-13
#SBATCH --time=1:00:00
#SBATCH --partition=rome
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --account=cpuuva006

set -e

cd /home/scur0742/Flood-News-Data-Pipeline
source /home/scur0742/venv-agent/bin/activate

START=$(( SLURM_ARRAY_TASK_ID * 20 + 1 ))
END=$(( (SLURM_ARRAY_TASK_ID + 1) * 20 ))
(( END > 269 )) && END=269

echo "=============================="
echo "Stage 09 — array task ${SLURM_ARRAY_TASK_ID} / floods ${START}–${END}"
echo "Running on $(hostname)"
echo "Start time: $(date)"
echo "=============================="

python stage_09_classify.py \
    --flood-id-start "$START" \
    --flood-id-end   "$END"

echo ""
echo "=============================="
echo "Task ${SLURM_ARRAY_TASK_ID} complete (floods ${START}–${END})"
echo "End time: $(date)"
echo "=============================="
