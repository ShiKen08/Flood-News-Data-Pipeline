#!/bin/bash
# ============================================================
# Flood Pipeline — Fine-tune Classifier (SLURM GPU job)
#
# Submit:  sbatch run_finetune.sh
# Monitor: squeue -u $USER
# Logs:    tail -f logs/finetune_%j.out
# ============================================================

#SBATCH --job-name=flood_finetune
#SBATCH --output=/home/scur0742/Flood-News-Data-Pipeline/logs/finetune_%j.out
#SBATCH --error=/home/scur0742/Flood-News-Data-Pipeline/logs/finetune_%j.err
#SBATCH --time=90:00:00
#SBATCH --gpus=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G

set -e

cd /home/scur0742/Flood-News-Data-Pipeline

source /home/scur0742/venv-agent/bin/activate

echo "=============================="
echo "Running on $(hostname)"
echo "Python: $(which python)"
echo "GPU: $(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null || echo 'no nvidia-smi')"
echo "Start time: $(date)"
echo "=============================="

echo ""
echo "--- Installing classifier dependencies ---"
python -m pip install -q setfit datasets accelerate

echo ""
echo "--- Removing old model weights (retraining with LaBSE) ---"
if [ -d "classifier/model" ]; then
    rm -rf classifier/model
    echo "  Old model removed"
fi

echo ""
echo "--- Building annotation set (if not already done) ---"
if [ ! -f data/annotation_batch_1.csv ]; then
    python scripts/build_annotation_set.py --n 300
    echo "IMPORTANT: Label data/annotation_batch_1.csv before continuing!"
    echo "           Fill the 'label' column (1=yes, 0=no) then resubmit."
    exit 0
fi

# Check if annotation batch has been labeled
LABELED=$(python -c "
import pandas as pd, sys
df = pd.read_csv('data/annotation_batch_1.csv')
n = df['label'].isin([0,1,'0','1']).sum()
print(n)
")
echo "Labeled examples found: $LABELED"

if [ "$LABELED" -lt 30 ]; then
    echo "ERROR: Need at least 30 labeled rows in data/annotation_batch_1.csv"
    echo "       Open the CSV, fill the 'label' column, then resubmit."
    exit 1
fi

echo ""
echo "--- Stage: Fine-tune classifier ---"
python classifier/finetune.py

echo ""
echo "--- Stage: Evaluate classifier ---"
python classifier/evaluate.py

echo ""
echo "--- Stage: Run inference on clean_text.parquet ---"
python stage_09_classify.py --pilot

echo ""
echo "=============================="
echo "Fine-tuning complete"
echo "Model saved to: classifier/model/"
echo "End time: $(date)"
echo "=============================="
