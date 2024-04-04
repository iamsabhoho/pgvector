
#
# imports
#

# import postgres python package
import psycopg2
import numpy as np
import datetime
from tqdm import tqdm
import os, sys
import pandas as pd
import argparse
import traceback

# parse arguments
parser = argparse.ArgumentParser("pgvector benchmarking tool.")
parser.add_argument('-d','--dataset', required=True, help="dataset name like 'deep-10M'")
parser.add_argument('-m',type=int, default=16) 
parser.add_argument('-e',type=int, default=64) # ef construction
parser.add_argument('-w','--workers', type=int, default=-1)  # workers
parser.add_argument('-g','--mem', type=int, default=-1)
parser.add_argument('-o','--output', required=True, help="output directory")
parser.add_argument('-c','--cpunodebind', type=int)
parser.add_argument('-p','--preferred', type=int)
parser.add_argument('-r','--remove', action='store_true', default=False)
args = parser.parse_args()
print(args)

# check output dir
"""if os.path.exists( args.output ):
    raise Exception("ERROR: output directory already exists.")
os.makedirs(args.output, exist_ok=False)
print("Created directory at", args.output)"""

#
# configuration settings
#

DATA_PATH = "/home/gwilliams/Projects/GXL/{}.npy".format(args.dataset)
GT_DIR = "/mnt/nas1/fvs_benchmark_datasets"
query_path = '/home/gwilliams/Projects/GXL/deep-queries-1000.npy'
queries = np.load(query_path, allow_pickle=True)
GT_PATH = os.path.join(GT_DIR, "{}-gt-1000.npy").format(args.dataset)
gt = np.load(GT_PATH, allow_pickle=True)


#
# helper functions
#
def compute_recall(a, b):
    '''Computes the recall metric on query results.'''

    nq, rank = a.shape
    intersect = [ np.intersect1d(a[i, :rank], b[i, :rank]).size for i in range(nq) ]
    ninter = sum( intersect )
    return ninter / a.size, intersect

def size_num(s):
    '''get raw numercs of text abbrev'''
    if s == '1M': return 1000000
    elif s == '2M': return 2000000
    elif s == '5M': return 5000000
    elif s == '10M': return 10000000
    elif s == '20M': return 20000000
    elif s == '50M': return 50000000
    elif s == '100M': return 100000000
    elif s == '200M': return 200000000
    elif s == '250M': return 250000000
    elif s == '500M': return 500000000
    elif s == '10K': return 10000
    elif s == '1000M': return 1000000000
    else: raise Exception("Unsupported size " + s)

#
# config
#
DIM = 96
M = args.m
EFC = args.e
worker = args.workers
mems = args.mem
results = []
basename = os.path.basename(DATA_PATH).split(".")[0]
numrecs = basename.split("-")[1]
num_records = size_num(numrecs)
ef_search = [64, 128, 256, 512]

save_path = './results/five/pgvector_%s_%d_%d_%d.csv'%(basename, EFC, M, worker)
print("CSV save path=", save_path)


# connect to local postgres server
conn = psycopg2.connect("host=localhost port=5432 dbname=postgres user=postgres password=gsi4ever target_session_attrs=read-write")
print(conn)

# Creating a cursor object
cursor = conn.cursor()

# create if doesnt exist
# enable extensions
# once per database
sql = "CREATE EXTENSION IF NOT EXISTS vector"
cursor.execute(sql)
print("created extensions")

# create if does not exist
# create table
# TODO
sql = "CREATE TABLE test (id bigserial PRIMARY KEY, embedding vector({}))".format(DIM)
cursor.execute(sql)
print("table created successfully!")


# insert vectors
# populate the table with vectors - iterate deep1B subset (10K) in a for loop and add rows (w embbeding column) to the table
# TODO
# load embeddings
data = np.load(DATA_PATH, allow_pickle=True)

start_time = datetime.datetime.now()

for i in tqdm(range(len(data))):
    s = str(list(data[i]))
    #print(s)
    sql = "INSERT INTO test (id, embedding) VALUES ({}, '{}')".format(i, s)
    #print(sql)
    cursor.execute(sql)
    #print("inserting {} vector".format(i))

add_time = datetime.datetime.now()
print("insert time: ", (add_time-start_time).total_seconds())


if worker >= 0:
    # set parallel workers
    sql = "SET max_parallel_maintenance_workers = {}".format(worker)
    cursor.execute(sql)

    sql = "SET max_parallel_workers = {}".format(worker)
    cursor.execute(sql)

if mems >= 0:
    sql = "SET maintenance_work_mem = '{}GB'".format(mems)
    cursor.execute(sql)

sql = "SET client_min_messages = DEBUG"
cursor.execute(sql)
sql = "SHOW client_min_messages"
cursor.execute(sql)
print("client_min_messages: ", cursor.fetchall())


# get number of maintenance workers
sql = "SHOW max_parallel_maintenance_workers"
cursor.execute(sql)
print("max parallel maintenance workers: ", cursor.fetchall())

# get number of workers
sql = "SHOW max_parallel_workers"
cursor.execute(sql)
print("max parallel workers: ", cursor.fetchall())

# get maintenance work mem
sql = "SHOW maintenance_work_mem"
cursor.execute(sql)
print("maintenance work mem: ", cursor.fetchall())

# show shared buffers
sql = "SHOW shared_buffers"
cursor.execute(sql)
buff = cursor.fetchall()
print("shared buffers: ", buff)

# create the HNSW index with cosine distance, M, and EFC on the table
# TODO

sql = "CREATE INDEX ON test USING hnsw (embedding vector_cosine_ops) WITH (m = {}, ef_construction = {})".format(M, EFC)
cursor.execute(sql)

end_time = datetime.datetime.now()

print("hnsw index created successfully!")
print("build time: ", (end_time-start_time).total_seconds())

results.append({'operation':'build', 'start_time':start_time, 'end_time':end_time,\
        'walltime':(end_time-start_time).total_seconds(), 'insert_time': (add_time-start_time).total_seconds(), 'units':'seconds',\
        'dataset':basename, 'numrecs':num_records,'ef_construction':EFC,\
        'M':M, 'ef_search':-1, 'labels':-1, 'distances':-1, 'memory':-1, 'workers':worker, 'buffer':buff})

for ef in ef_search:
    sql = "SET hnsw.ef_search = {}".format(ef)
    cursor.execute(sql)

    for i in range(len(queries)):
        
        query = queries[i]

        # cosine distances
        q = str(list(query))
        start_time = datetime.datetime.now()
        sql = "SELECT id, (embedding <=> '{}') AS cosine_similarity FROM test ORDER BY cosine_similarity LIMIT 10".format(q)
        ## 
        cursor.execute(sql)
        res = cursor.fetchall()
        end_time = datetime.datetime.now()

        # compute recall
        lbl_lst = np.array([j[0] for j in res]).reshape(1, 10)
        dist_lst = np.array([j[1] for j in res]).reshape(1, 10)
        #gt_lst = gt[i][:10].reshape(1, 10)
        #r = compute_recall(lbl_lst, gt_lst)

        results.append({'operation':'search', 'start_time':start_time, \
                'end_time':end_time, 'walltime':((end_time-start_time).total_seconds() * 1000 ),\
                'insert_time':-1, 'units':'milliseconds', 'dataset':basename, 'numrecs':num_records,\
                'ef_construction':-1, 'M':-1, 'ef_search':ef, 'labels':lbl_lst, \
                'distances':dist_lst, 'memory':-1, 'workers':worker, 'buffer':buff})

df = pd.DataFrame(results)
df.to_csv(save_path, sep="\t")
print("done saving to csv", save_path)
df = pd.read_csv(save_path, delimiter="\t")
print(df.head())


# delete table
# TODO
sql = "DROP TABLE test"
cursor.execute(sql)
print("table dropped successfully!")

try:
    conn.commit()
    conn.close()
except:
    traceback.print_exc()

print("DONE")
sys.exit(0)
