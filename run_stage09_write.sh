#!/bin/bash
# ============================================================
# Flood Pipeline — Stage 09 Phase B: CSV write only
#
# Loads model_scores.parquet (written by Phase A) + full
# clean_text.parquet. No model in memory — writes verified CSV.
#
# Submit with dependency on Phase A:
#   INFER=$(sbatch --parsable run_stage09_infer.sh)
#   sbatch --dependency=afterok:$INFER run_stage09_write.sh
# ============================================================

#SBATCH --job-name=s09_write
#SBATCH --output=/home/scur0742/Flood-News-Data-Pipeline/logs/s09_write_%j.out
#SBATCH --error=/home/scur0742/Flood-News-Data-Pipeline/logs/s09_write_%j.err
#SBATCH --time=0:30:00
#SBATCH --partition=rome
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --account=cpuuva006

set -e

cd /home/scur0742/Flood-News-Data-Pipeline
source /home/scur0742/venv-agent/bin/activate

echo "=============================="
echo "Stage 09 — Phase B: CSV write"
echo "Running on $(hostname)"
echo "Start time: $(date)"
echo "=============================="

python stage_09_classify.py --write-only

echo ""
echo "=============================="
echo "Phase B complete — verified CSV written"
echo "End time: $(date)"
echo "=============================="
