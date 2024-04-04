#!/bin/bash

#set -e
set -x


###################
#       50M        #
###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-50M"
WORKERS=10
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"
