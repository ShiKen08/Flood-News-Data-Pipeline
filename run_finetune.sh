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
#SBATCH --partition=rome
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --account=cpuuva006

set -e

cd /home/scur0742/Flood-News-Data-Pipeline

source /home/scur0742/venv-agent/bin/activate

export OMP_NUM_THREADS=8
export MKL_NUM_THREADS=8

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
echo "--- Removing old model weights (retraining from scratch) ---"
if [ -d "classifier/model" ]; then
    rm -rf classifier/model
    echo "  Old model removed"
fi

echo ""
echo "--- Checking annotation batches ---"
if [ ! -f data/annotation_batch_1.csv ]; then
    echo "ERROR: data/annotation_batch_1.csv not found"
    echo "       Run: python scripts/build_annotation_set.py --n 300"
    exit 1
fi

# Count labeled rows across all annotation batches
LABELED=$(python -c "
import pandas as pd, glob
files = sorted(glob.glob('data/annotation_batch_*.csv'))
total = 0
for f in files:
    df = pd.read_csv(f)
    n = df['label'].isin([0,1,'0','1']).sum()
    print(f'  {f}: {n} labeled rows', flush=True)
    total += n
print(total)
" | tail -1)
echo "Total labeled examples: $LABELED"

if [ "$LABELED" -lt 30 ]; then
    echo "ERROR: Need at least 30 labeled rows across annotation batches"
    exit 1
fi

echo ""
echo "--- Stage: Fine-tune classifier ---"
python classifier/finetune.py \
    --iters 5 \
    --batch-size 16

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
