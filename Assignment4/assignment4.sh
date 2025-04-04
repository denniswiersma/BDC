#!/bin/bash
#SBATCH --job-name=bdc4
#SBATCH --output=assignment4.out
#SBATCH --error=assignment4.err
#SBATCH --nodes=1
#SBATCH --partition=assemblix
#SBATCH --nodelist=assemblix2019
#SBATCH --ntasks=5
#SBATCH --time=01:00:00
#SBATCH --mem=64G


FASTQ_PATH=/students/2023-2024/Thema12/dwiersma_BDC/BDC/rnaseq.fastq
SCRIPT_PATH=/students/2023-2024/Thema12/dwiersma_BDC/BDC/Assignment4/assignment4.py

for workers in {1..4}; do
  for rep in {1..3}; do
    echo "Running with $workers workers, repetition $rep"
    mpirun -np $((workers+1)) python3 "$SCRIPT_PATH" \
      -o "results_w${workers}_r${rep}.csv" \
      "$FASTQ_PATH"
      # --run-id "$rep" \
  done
done
