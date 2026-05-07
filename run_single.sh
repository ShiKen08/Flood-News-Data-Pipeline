#!/bin/bash

# ============================
# ssh scur0742@snellius.surf.nl
#
# How to submit:
# sbatch run_single.sh
#
# Check job:
# squeue
#
# Cancel job:
# scancel <jobid>
# 
# Check logs example:
# tail -f /home/scur0742/Flood-News-Data-Pipeline/logs/<jobid>_<taskid>.out
# 
# Copy result example:
# scp -r scur0742@snellius.surf.nl:/home/scur0742/Flood-News-Data-Pipeline/output/ .
# ============================

#SBATCH --job-name=group_water
#SBATCH --output=/hme/scur0742/Flood-News-Data-Pipeline/logs/k_%j.out
#SBATCH --error=/home/scur0742/Flood-News-Data-Pipeline/logs/k_%j.err
#SBATCH --time=30:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G

cd /home/scur0742/kun/Flood-News-Data-Pipeline

source /home/scur0742/venv-agent/bin/activate

echo "Running on $(hostname)"
echo "Start time: $(date)"

python setup.py
pip install -r requirements.txt
python stage_00_preflight.py
python stage_01_query_specs.py
python stage_02_query_cc_index.py
python stage_03_validate_pointers.py
python stage_04_download_warc.py
python stage_05_extract_text.py
python stage_06v_clean_deduplicate.py
python stage_07_url_report.py
python stage_08_nlp_analysis.py

echo "End time: $(date)"