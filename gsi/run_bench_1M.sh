#!/bin/bash

#set -e
set -x


# get name of timestamped output dir
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT

# run pgvector
DATASET="deep-1M"
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --mem $MEMORY | tee "$OUTPUT/$DATASET.log"



# get name of timestamped output dir
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT

DATASET="deep-1M"
WORKERS=10
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"



# get name of timestamped output dir
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT

DATASET="deep-1M"
WORKERS=20
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"
