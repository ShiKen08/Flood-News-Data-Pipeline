#!/bin/bash
# ============================================================
# Flood Pipeline — Late-Window Batch (SLURM job)
#
# Queries CC crawls that landed 2-5 months AFTER each flood event,
# capturing articles published promptly but indexed by CCBot late.
# Runs the full pipeline (stages 00-09) with the late window config.
#
# Submit one batch:
#   sbatch run_single_late.sh 1 40       # floods 1-40
#   sbatch run_single_late.sh 41 80      # floods 41-80
#   sbatch run_single_late.sh 81 150     # floods 81-150
#   sbatch run_single_late.sh 151 269    # floods 151-269
#
# Monitor: squeue -u $USER
# Logs:    tail -f logs/late_<jobid>.out
# Merge:   python3 scripts/merge_batch_outputs.py
# ============================================================

#SBATCH --job-name=flood_late
#SBATCH --output=/home/scur0742/Flood-News-Data-Pipeline/logs/late_%j.out
#SBATCH --error=/home/scur0742/Flood-News-Data-Pipeline/logs/late_%j.err
#SBATCH --time=30:00:00
#SBATCH --partition=rome
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --account=cpuuva006

set -e

cd /home/scur0742/Flood-News-Data-Pipeline

source /home/scur0742/venv-agent/bin/activate

# ---- Flood range & per-batch output dir --------------------------------
if [ -n "$1" ] && [ -n "$2" ]; then
    export PIPELINE_FLOOD_IDS="${1}-${2}"
    export PIPELINE_OUTPUT_DIR="$(pwd)/output/batch_late_${1}_${2}"
elif [ -z "$PIPELINE_FLOOD_IDS" ]; then
    export PIPELINE_FLOOD_IDS=""
    export PIPELINE_OUTPUT_DIR="$(pwd)/output"
fi
mkdir -p "${PIPELINE_OUTPUT_DIR}"

# Signal downstream stages to use crawl_coverage_late.parquet
export PIPELINE_LATE_WINDOW=1
# ------------------------------------------------------------------------

echo "=============================="
echo "Running on $(hostname)"
echo "Python: $(which python)"
echo "Flood IDs : ${PIPELINE_FLOOD_IDS:-'(config.py default)'}"
echo "Output dir: ${PIPELINE_OUTPUT_DIR}"
echo "Window    : LATE (+60d to +150d post-event)"
echo "Start time: $(date)"
echo "=============================="

python -m pip install -q -r requirements.txt

echo ""
echo "--- Stage 00: Preflight (late window) ---"
# Always regenerate for late window — crawl_coverage_late.parquet may not exist
if [ ! -f "output/crawl_coverage_late.parquet" ]; then
    python stage_00_preflight.py --late-window
else
    echo "  crawl_coverage_late.parquet exists — skipping (delete to force rerun)"
fi

echo ""
echo "--- Stage 01: Query specs (reading crawl_coverage_late.parquet) ---"
python stage_01_query_specs.py

echo ""
echo "--- Stage 02: CC index queries ---"
python stage_02_query_cc_index.py

echo ""
echo "--- Stage 03: Validate pointers ---"
python stage_03_validate_pointers.py

echo ""
echo "--- Stage 04: Download WARC ---"
python stage_04_download_warc.py --full

echo ""
echo "--- Stage 05: Extract text ---"
python stage_05_extract_text.py

echo ""
echo "--- Stage 06: Clean & deduplicate ---"
python stage_06v_clean_deduplicate.py

echo ""
echo "--- Stage 07: URL report ---"
python stage_07_url_report.py --pilot --event-articles --domain-cap 15

echo ""
echo "--- Stage 08: NLP bridge ---"
python stage_08_nlp_analysis.py --event-articles --pilot

echo ""
echo "--- Stage 09: ML classification ---"
python stage_09_classify.py

echo ""
echo "=============================="
echo "Late-window pipeline complete"
echo "Output: ${PIPELINE_OUTPUT_DIR}"
echo "End time: $(date)"
echo "=============================="
