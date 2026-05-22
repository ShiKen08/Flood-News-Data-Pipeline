#!/bin/bash
# ============================================================
# Flood Pipeline — Stage 09 ML Classification (SLURM job)
#
# Submit after merge_batch_outputs.py has run:
#   sbatch run_stage09.sh
#
# Monitor: squeue -u $USER
# Logs:    tail -f logs/stage09_<jobid>.out
# ============================================================

#SBATCH --job-name=flood_stage09
#SBATCH --output=/home/scur0742/Flood-News-Data-Pipeline/logs/stage09_%j.out
#SBATCH --error=/home/scur0742/Flood-News-Data-Pipeline/logs/stage09_%j.err
#SBATCH --time=4:00:00
#SBATCH --partition=rome
#SBATCH --cpus-per-task=4
#SBATCH --mem=48G
#SBATCH --account=cpuuva006

set -e

cd /home/scur0742/Flood-News-Data-Pipeline

source /home/scur0742/venv-agent/bin/activate

echo "=============================="
echo "Running on $(hostname)"
echo "Python: $(which python)"
echo "Start time: $(date)"
echo "=============================="

python -m pip install -q -r requirements.txt

echo ""
echo "--- Stage 09: ML classification ---"
python stage_09_classify.py

echo ""
echo "=============================="
echo "Stage 09 complete"
echo "End time: $(date)"
echo "=============================="
