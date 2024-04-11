#!/bin/bash

source /home/sho/miniconda3/etc/profile.d/conda.sh
conda activate pgvector

python3 --version

export PGPASSWORD="gsi4ever"
psql -d postgres -h localhost -p 5432 -U postgres -c "drop table test"
./run_10K_insertonly.sh
TIMESTAMP=$(date +%s)
psql -d postgres -h localhost -p 5432 -U postgres -a -e -S -f test.sql -v ON_ERROR_STOP=on 2>&1 | tee ./results/output/output_10k_$TIMESTAMP.log



psql -d postgres -h localhost -p 5432 -U postgres -c "drop table test"
./run_1M_insertonly.sh
TIMESTAMP=$(date +%s)
psql -d postgres -h localhost -p 5432 -U postgres -a -e -S -f test.sql -v ON_ERROR_STOP=on 2>&1 | tee ./results/output/output_1M_$TIMESTAMP.log



psql -d postgres -h localhost -p 5432 -U postgres -c "drop table test"
./run_5M_insertonly.sh
TIMESTAMP=$(date +%s)
psql -d postgres -h localhost -p 5432 -U postgres -a -e -S -f test.sql -v ON_ERROR_STOP=on 2>&1 | tee ./results/output/output_5M_$TIMESTAMP.log



psql -d postgres -h localhost -p 5432 -U postgres -c "drop table test"
./run_10M_insertonly.sh
TIMESTAMP=$(date +%s)
psql -d postgres -h localhost -p 5432 -U postgres -a -e -S -f test.sql -v ON_ERROR_STOP=on 2>&1 | tee ./results/output/output_10M_$TIMESTAMP.log


psql -d postgres -h localhost -p 5432 -U postgres -c "drop table test"
./run_20M_insertonly.sh
TIMESTAMP=$(date +%s)
psql -d postgres -h localhost -p 5432 -U postgres -a -e -S -f test.sql -v ON_ERROR_STOP=on 2>&1 | tee ./results/output/output_20M_$TIMESTAMP.log
