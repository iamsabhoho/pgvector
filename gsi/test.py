
#
# imports
#

# import postgres python package
import psycopg2

#
# configuration settings
#

# connect to local postgres server
conn = psycopg2.connect("host=localhost port=5432 dbname=postgres user=postgres password=gsi4ever target_session_attrs=read-write")
print(conn)

# create new database
# TODO

# create table
# TODO

# create the HNSW index with cosine distance, M, and EFC on the table
# TODO

# populate the table with vectors - iterate deep1B subset (10K) in a for loop and add rows (w embbeding column) to the table
# TODO

# delete database
# TODO
