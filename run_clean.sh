#!/bin/bash
# ============================================================
# Flood Pipeline — Stage 06v Clean & Deduplicate (SLURM job)
#
# Reruns stage_06v on existing extracted_text.parquet data,
# splitting by flood ID range for parallel execution.
# Stage_06v reads inputs from the main output/ dir and writes
# results to a per-batch subdir so parallel jobs never conflict.
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

MAIN_OUTPUT="$(pwd)/output"

# ---- Flood range & per-batch output dir --------------------------------
if [ -n "$1" ] && [ -n "$2" ]; then
    FLOOD_START="$1"
    FLOOD_END="$2"
    BATCH_DIR="${MAIN_OUTPUT}/batch_clean_${FLOOD_START}_${FLOOD_END}"
else
    echo "ERROR: Usage: sbatch run_clean.sh START END  (e.g. sbatch run_clean.sh 1 40)"
    exit 1
fi

mkdir -p "${BATCH_DIR}"

# Stage_06v reads inputs from OUTPUT_DIR — symlink shared input parquets
# into the batch dir so it can find them without conflicting with other jobs
for f in extracted_text.parquet validated_pointers.parquet event_query_specs.parquet \
          warc_fetch_log.parquet; do
    if [ -f "${MAIN_OUTPUT}/${f}" ] && [ ! -e "${BATCH_DIR}/${f}" ]; then
        ln -s "${MAIN_OUTPUT}/${f}" "${BATCH_DIR}/${f}"
    fi
done

export PIPELINE_OUTPUT_DIR="${BATCH_DIR}"
# ------------------------------------------------------------------------

echo "=============================="
echo "Running on $(hostname)"
echo "Python: $(which python)"
echo "Flood IDs : ${FLOOD_START}-${FLOOD_END}"
echo "Batch dir : ${BATCH_DIR}"
echo "Start time: $(date)"
echo "=============================="

echo ""
echo "--- Installing dependencies ---"
python -m pip install -q -r requirements.txt

echo ""
echo "--- Stage 06v: Clean & deduplicate (floods ${FLOOD_START}-${FLOOD_END}) ---"
python stage_06v_clean_deduplicate.py --fresh --flood-ids "$(seq -s, ${FLOOD_START} ${FLOOD_END})"

echo ""
echo "=============================="
echo "Stage 06v complete"
echo "Output: ${BATCH_DIR}/clean_text.parquet"
echo "End time: $(date)"
echo "=============================="
