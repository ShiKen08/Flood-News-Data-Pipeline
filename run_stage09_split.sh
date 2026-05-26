#!/bin/bash
# ============================================================
# Flood Pipeline — Stage 09 ML Classification (split SLURM jobs)
#
# Splits the 269 floods across 4 parallel jobs to avoid OOM.
# Each job writes its own verified CSV; merge with:
#   python3 scripts/merge_stage09_splits.py
#
# Submit all 4 jobs at once:
#   bash run_stage09_split.sh
#
# Or submit a single range manually:
#   sbatch --export=ALL,START=1,END=65     run_stage09_split.sh
#   sbatch --export=ALL,START=66,END=130   run_stage09_split.sh
#   sbatch --export=ALL,START=131,END=200  run_stage09_split.sh
#   sbatch --export=ALL,START=201,END=269  run_stage09_split.sh
# ============================================================

#SBATCH --job-name=flood_s09
#SBATCH --output=/home/scur0742/Flood-News-Data-Pipeline/logs/stage09_split_%j.out
#SBATCH --error=/home/scur0742/Flood-News-Data-Pipeline/logs/stage09_split_%j.err
#SBATCH --time=2:00:00
#SBATCH --partition=rome
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --account=cpuuva006

set -e

cd /home/scur0742/Flood-News-Data-Pipeline

source /home/scur0742/venv-agent/bin/activate

echo "=============================="
echo "Running on $(hostname)"
echo "Python: $(which python)"
echo "Flood range: ${START:-1} to ${END:-269}"
echo "Start time: $(date)"
echo "=============================="

python stage_09_classify.py \
    --flood-id-start "${START:-1}" \
    --flood-id-end   "${END:-269}"

echo ""
echo "=============================="
echo "Stage 09 split complete (floods ${START:-1}-${END:-269})"
echo "End time: $(date)"
echo "=============================="

# ---- If run as a script (not sbatch), submit all 4 jobs ----
if [[ "${BASH_SOURCE[0]}" == "${0}" ]] && [[ -z "$SLURM_JOB_ID" ]]; then
    echo "Submitting 4 parallel stage_09 split jobs..."
    sbatch --export=ALL,START=1,END=65     run_stage09_split.sh
    sbatch --export=ALL,START=66,END=130   run_stage09_split.sh
    sbatch --export=ALL,START=131,END=200  run_stage09_split.sh
    sbatch --export=ALL,START=201,END=269  run_stage09_split.sh
    echo "Submitted. Monitor with: squeue -u \$USER"
fi
