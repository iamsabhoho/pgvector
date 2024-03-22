#!/bin/bash

set -e
set -x

#
# setup and config
#

# get name of timestamped output dir
TIMESTAMP=$(date +%s)
OUTPUT="/tmp/gxl_$TIMESTAMP"

#
# Run pgvector commands
#

# run pgvector
DATASET="deep-1M"
WORKER=200
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKER | tee "$OUTPUT/$DATASET.log"