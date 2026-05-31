#!/bin/bash
# ============================================================
# Flood Pipeline — Stage 09 Phase A: Inference only
#
# Loads ONLY 4 columns (doc_id, flood_id, page_title, clean_text),
# runs SetFit classifier, saves output/model_scores.parquet, exits.
# No CSV writing — model is fully freed before Phase B starts.
#
# Submit both phases with dependency:
#   INFER=$(sbatch --parsable run_stage09_infer.sh)
#   sbatch --dependency=afterok:$INFER run_stage09_write.sh
# ============================================================

#SBATCH --job-name=s09_infer
#SBATCH --output=/home/scur0742/Flood-News-Data-Pipeline/logs/s09_infer_%j.out
#SBATCH --error=/home/scur0742/Flood-News-Data-Pipeline/logs/s09_infer_%j.err
#SBATCH --time=2:00:00
#SBATCH --partition=rome
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --account=cpuuva006

set -e

cd /home/scur0742/Flood-News-Data-Pipeline
source /home/scur0742/venv-agent/bin/activate

echo "=============================="
echo "Stage 09 — Phase A: Inference"
echo "Running on $(hostname)"
echo "Start time: $(date)"
echo "=============================="

python stage_09_classify.py --infer-only

echo ""
echo "=============================="
echo "Phase A complete — model_scores.parquet written"
echo "End time: $(date)"
echo "=============================="
