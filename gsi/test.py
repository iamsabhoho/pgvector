
#
# imports
#

# import postgres python package
import psycopg2
import numpy as np
import datetime
from tqdm import tqdm




#
# configuration settings
#
DATA_PATH = "/home/gwilliams/Projects/GXL/deep-10K.npy"
query_path = '/home/gwilliams/Projects/GXL/deep-queries-1000.npy'
queries = np.load(query_path, allow_pickle=True)

DIM = 96
M = 32
EFC = 64

#
# helper functions
#
def compute_recall(a, b):
    '''Computes the recall metric on query results.'''

    nq, rank = a.shape
    intersect = [ np.intersect1d(a[i, :rank], b[i, :rank]).size for i in range(nq) ]
    ninter = sum( intersect )
    return ninter / a.size, intersect

# connect to local postgres server
conn = psycopg2.connect("host=localhost port=5432 dbname=postgres user=postgres password=gsi4ever target_session_attrs=read-write")
print(conn)

# Creating a cursor object
cursor = conn.cursor()

"""# create new database
# TODO
sql = "CREATE DATABASE test"
cursor.execute(sql)
print("database created successfully!")"""


# create if doesnt exist
# enable extensions
# once per database
sql = "CREATE EXTENSION IF NOT EXISTS vector"
cursor.execute(sql)
print("created extensions")

# create if does not exist
# create table
# TODO
sql = "CREATE TABLE IF NOT EXISTS test (id bigserial PRIMARY KEY, embedding vector({}))".format(DIM)
cursor.execute(sql)
print("table created successfully!")


# insert vectors
# populate the table with vectors - iterate deep1B subset (10K) in a for loop and add rows (w embbeding column) to the table
# TODO
# load embeddings
data = np.load(DATA_PATH, allow_pickle=True)

for i in tqdm(range(len(data))):
    s = str(list(data[i]))
    #print(s)
    sql = "INSERT INTO test (i, embedding) VALUES ({}, '{}')".format(i, s)
    #print(sql)
    cursor.execute(sql)
    #print("inserting {} vector".format(i))


# create the HNSW index with cosine distance, M, and EFC on the table
# TODO
sql = "CREATE INDEX ON test USING hnsw (embedding vector_cosine_ops) WITH (m = {}, ef_construction = {})".format(M, EFC)
cursor.execute(sql)
print("hnsw index created successfully!")

sql = "SET hnsw.ef_search = 100"
cursor.execute(sql)

results = {}

for query in queries:
    start_time = datetime.datetime.now()

    # cosine distances
    q = str(list(query))
    sql = "SELECT 1 - (embedding <=> '{}') AS cosine_similarity FROM test ORDER BY cosine_similarity LIMIT 10".format(q)
    cursor.execute(sql)
    res = cursor.fetchall()
    print(res)
    break

    end_time = datetime.datetime.now()
    
    t = end_time - start_time


recall = []
# recall



"""
# delete database
# TODO
sql = "DROP DATABASE test"
cursor.execute(sql)
print("table dropped successfully!")
"""
conn.close()
