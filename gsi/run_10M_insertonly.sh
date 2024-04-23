#!/bin/bash

set -e
set -x

#
# setup and config
#

# get name of timestamped output dir
TIMESTAMP=$(date +%s)
OUTPUT="./results/output_insert/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
#
# Run pgvector commands
#

# run pgvector
DATASET="deep-10M"
MEMORY=100
WORKERS=32
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --mem $MEMORY --workers $WORKERS --stop_after_insert --verbose | tee "$OUTPUT/$DATASET.log"