#!/bin/bash

source /home/sho/miniconda3/etc/profile.d/conda.sh
conda activate pgvector

python3 --version

export PGPASSWORD="gsi4ever"
psql -d postgres -h localhost -p 5432 -U postgres -c "drop table test"
./run_250M_insertonly.sh
TIMESTAMP=$(date +%s)
psql -d postgres -h localhost -p 5432 -U postgres -a -e -S -f test.sql -v ON_ERROR_STOP=on 2>&1 | tee ./results/output_0508/output_250M_$TIMESTAMP.log
