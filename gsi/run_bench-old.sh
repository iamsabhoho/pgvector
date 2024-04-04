#!/bin/bash

#set -e
set -x

###################
#       1M        #
###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-1M"
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-1M"
WORKERS=2
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-1M"
WORKERS=4
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-1M"
WORKERS=8
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-1M"
WORKERS=16
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"


###################
#       5M        #
###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-5M"
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-5M"
WORKERS=2
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-5M"
WORKERS=4
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-5M"
WORKERS=8
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-5M"
WORKERS=16
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"


###################
#       10M        #
###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-10M"
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-10M"
WORKERS=2
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-10M"
WORKERS=4
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-10M"
WORKERS=8
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-10M"
WORKERS=16
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
#       20M        #
###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-20M"
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-20M"
WORKERS=2
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-20M"
WORKERS=4
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-20M"
WORKERS=8
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

###################
TIMESTAMP=$(date +%s)
OUTPUT="./results/gxl_$TIMESTAMP"
mkdir -p $OUTPUT
DATASET="deep-20M"
WORKERS=16
MEMORY=8
echo "Running pgvector on $DATASET"
echo "Writing output to $OUTPUT"
python -u pg_bench.py --dataset $DATASET --output $OUTPUT --workers $WORKERS --mem $MEMORY | tee "$OUTPUT/$DATASET.log"

