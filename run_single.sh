#!/bin/bash
# ============================================================
# Flood Pipeline — SLURM job script
#
# Submit one batch:
#   sbatch run_single.sh 1 20      # floods 1–20
#   sbatch run_single.sh 21 40     # floods 21–40  (accumulates with previous)
#   sbatch run_single.sh 41 60     # floods 41–60
#
# Or with a comma list:
#   PIPELINE_FLOOD_IDS="1,5,10" sbatch run_single.sh
#
# Monitor: squeue -u $USER
# Logs:    tail -f logs/k_<jobid>.out
# Cancel:  scancel <jobid>
# Copy:    scp -r scur0742@snellius.surf.nl:~/Flood-News-Data-Pipeline/output/ .
# ============================================================

#SBATCH --job-name=flood_pipeline
#SBATCH --output=/home/scur0742/Flood-News-Data-Pipeline/logs/k_%j.out
#SBATCH --error=/home/scur0742/Flood-News-Data-Pipeline/logs/k_%j.err
#SBATCH --time=30:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G

set -e   # stop immediately if any stage fails

cd /home/scur0742/Flood-News-Data-Pipeline

source /home/scur0742/venv-agent/bin/activate

# ---- Flood range -------------------------------------------------------
# Two positional args: START END  (e.g. sbatch run_single.sh 1 20)
# Falls back to PIPELINE_FLOOD_IDS env var, then config.py default.
if [ -n "$1" ] && [ -n "$2" ]; then
    export PIPELINE_FLOOD_IDS="${1}-${2}"
elif [ -z "$PIPELINE_FLOOD_IDS" ]; then
    export PIPELINE_FLOOD_IDS=""   # use config.py default
fi
# ------------------------------------------------------------------------

echo "=============================="
echo "Running on $(hostname)"
echo "Python: $(which python)"
echo "Flood IDs: ${PIPELINE_FLOOD_IDS:-'(config.py default)'}"
echo "Start time: $(date)"
echo "=============================="

# --- one-time install (fast if already installed) ---
python -m pip install -q -r requirements.txt

echo ""
echo "--- Stage 00: Preflight ---"
if [ -f "output/crawl_coverage.parquet" ] && [ -f "output/location_dictionary.parquet" ] && [ -f "output/language_assignments.parquet" ]; then
    echo "  Stage 00 outputs already exist — skipping (delete output/*.parquet to force rerun)"
else
    python stage_00_preflight.py
fi

echo ""
echo "--- Stage 01: Query specs ---"
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
echo "Pipeline complete"
echo "End time: $(date)"
echo "=============================="
