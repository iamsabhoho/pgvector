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
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python3 -u pg_bench.py --dataset $DATASET --output $OUTPUT --mem $MEMORY --stop_after_insert --verbose | tee "$OUTPUT/$DATASET.log"