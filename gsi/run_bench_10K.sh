#!/bin/bash

set -e
set -x

#
# setup and config
#

# get name of timestamped output dir
TIMESTAMP=$(date +%s)
OUTPUT="/tmp/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
#
# Run pgvector commands
#

# run pgvector
DATASET="deep-10K"
WORKERS=10
MEMORY=1000
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"