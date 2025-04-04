#!/bin/bash

INPUT="/students/2023-2024/Thema12/dwiersma_BDC/BDC/rnaseq.fastq"

parallel --jobs 4 \
    --sshlogin nuc112,nuc113 \
    --pipepart \
    --recstart '@' \
    --block 1M \
    python3 /students/2023-2024/Thema12/dwiersma_BDC/BDC/Assignment3/assignment3.py --chunkmode :::: "$INPUT" | python3 /students/2023-2024/Thema12/dwiersma_BDC/BDC/Assignment3/assignment3.py --totalmode > output.csv
