#!/bin/bash
# ============================================================
# Flood Pipeline — Stage 06v Clean & Deduplicate (SLURM job)
#
# Reruns stage_06v on a batch of flood IDs, writing to a
# per-batch output directory so parallel jobs never conflict.
# Run scripts/merge_batch_outputs.py after all jobs finish.
#
# Submit a flood range:
#   sbatch run_clean.sh 1 40        # floods 1-40
#   sbatch run_clean.sh 41 80       # floods 41-80
#   sbatch run_clean.sh 81 150      # floods 81-150
#   sbatch run_clean.sh 151 269     # floods 151-269
#
# Submit all at once (copy-paste):
#   sbatch run_clean.sh 1 40
#   sbatch run_clean.sh 41 80
#   sbatch run_clean.sh 81 150
#   sbatch run_clean.sh 151 269
#
# Monitor:  squeue -u $USER
# Logs:     tail -f logs/clean_<jobid>.out
# Merge:    python3 scripts/merge_batch_outputs.py
# ============================================================

#SBATCH --job-name=flood_clean
#SBATCH --output=/home/scur0742/Flood-News-Data-Pipeline/logs/clean_%j.out
#SBATCH --error=/home/scur0742/Flood-News-Data-Pipeline/logs/clean_%j.err
#SBATCH --time=4:00:00
#SBATCH --partition=rome
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --account=cpuuva006

set -e

cd /home/scur0742/Flood-News-Data-Pipeline

source /home/scur0742/venv-agent/bin/activate

# ---- Flood range & per-batch output dir --------------------------------
if [ -n "$1" ] && [ -n "$2" ]; then
    export PIPELINE_FLOOD_IDS="${1}-${2}"
    export PIPELINE_OUTPUT_DIR="$(pwd)/output/batch_${1}_${2}"
elif [ -z "$PIPELINE_FLOOD_IDS" ]; then
    export PIPELINE_FLOOD_IDS=""
    export PIPELINE_OUTPUT_DIR="$(pwd)/output"
fi
mkdir -p "${PIPELINE_OUTPUT_DIR}"
# ------------------------------------------------------------------------

echo "=============================="
echo "Running on $(hostname)"
echo "Python: $(which python)"
echo "Flood IDs : ${PIPELINE_FLOOD_IDS:-'(all)'}"
echo "Output dir: ${PIPELINE_OUTPUT_DIR}"
echo "Start time: $(date)"
echo "=============================="

echo ""
echo "--- Installing dependencies ---"
python -m pip install -q -r requirements.txt

echo ""
echo "--- Stage 06v: Clean & deduplicate ---"
python stage_06v_clean_deduplicate.py --fresh

echo ""
echo "=============================="
echo "Stage 06v complete"
echo "Output: ${PIPELINE_OUTPUT_DIR}/clean_text.parquet"
echo "End time: $(date)"
echo "=============================="
