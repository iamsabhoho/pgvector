
#
# imports
#

# import postgres python package
#import psycopg2
import numpy as np
import datetime
from tqdm import tqdm
import os, sys
import pandas as pd
import argparse
import traceback
from subprocess import run, check_output, Popen, PIPE, STDOUT
import time
import psutil
import platform

from pgvector.psycopg import register_vector
import psycopg

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
parser.add_argument('-f','--stop_after_insert', action='store_true', default=False)
parser.add_argument('-v','--verbose', action='store_true', default=False)
args = parser.parse_args()
print("args=",args)

if args.verbose:
    verbose = args.verbose

# check output dir
"""if os.path.exists( args.output ):
    raise Exception("ERROR: output directory already exists.")
os.makedirs(args.output, exist_ok=False)
print("Created directory at", args.output)"""

#
# configuration settings
#

# apu12
DATA_PATH = "/home/gwilliams/Projects/GXL/{}.npy".format(args.dataset)
GT_DIR = "/mnt/nas1/fvs_benchmark_datasets"
query_path = '/home/gwilliams/Projects/GXL/deep-queries-1000.npy'
queries = np.load(query_path, allow_pickle=True)
GT_PATH = os.path.join(GT_DIR, "{}-gt-1000.npy").format(args.dataset)
gt = np.load(GT_PATH, allow_pickle=True)

# namibia
"""DATA_PATH = "/home/sho/deep1b/{}.npy".format(args.dataset)
query_path = '/home/sho/deep1b/deep-queries-1000.npy'
queries = np.load(query_path, allow_pickle=True)
GT_PATH = "/home/sho/deep1b/{}-gt-1000.npy".format(args.dataset)
gt = np.load(GT_PATH, allow_pickle=True)"""


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

def get_source_info(file_path):
    '''Get exhaustive information about a SAR source file'''

    # make sure its the absolute path
    abspath = os.path.abspath(file_path)

    # get drive info of path
    p = run("df %s" % abspath, shell=True, capture_output=True)
    if (p.returncode!=0):
        raise Exception("Could not retrieve hard drive info for file %s" % abspath)
    drvinfo = p.stdout.decode().split("\n")[1].split()
    if verbose: print("Drive info for %s" % abspath, "=", drvinfo)
    if require_local_path and not drvinfo[0].startswith("/dev"):
        raise Exception("Source file %s is not a local file" % abspath )

    # prepare the dict return obj
    return_dct = {'abspath':abspath, 'hdd': drvinfo[0]}

    # locate partition information for file
    partitions = psutil.disk_partitions()
    found_partition = False
    for partition in partitions:
        if partition.device==drvinfo[0]:
            found_partition=True
            break
    if not found_partition:
        raise Exception("Could not location partition for %s" % abspath)

    # get summary drives
    cmd = "hwinfo --short --disk"
    if verbose: print("Running cmd-",cmd)
    p = run(cmd, shell=True, capture_output=True)
    if p.returncode!=0:
        raise Exception("Could not run 'hwinfo' - Are you sure its installed?")
    drives_lines = p.stdout.decode().split("\n")
    print(drives_lines)
    drives = [ ln.strip().split()[0] for ln in drives_lines[1:] if ln.strip()!="" ]
    drives_dct = {}
    for drv in drives:
        drives_dct[drv] = drv
    print("Got hwinfo drives", drives_dct)

    # get drive manufacturer info
    #primdrv_part = ''.join("" if c.isdigit() else c for c in partition.device)
    primdrv_part = partition.device
    if verbose: print("Primary drive partition = ", primdrv_part)
    primdrv = None
    for drv in drives_dct.keys():
        if verbose: print("drv compare =",drv,primdrv_part)
        if primdrv_part.find(drv)==0:
            primdrv = drv
            if verbose: print("found!", drv, primdrv)
            break
    if primdrv == None:
        raise Exception("Could not get primary drive info for " + partition.device +" " + primdrv_part)
   
    p = run("hwinfo --disk --only %s" % primdrv, shell=True, capture_output=True)
    if p.returncode!=0:
        raise Exception("Could not run 'hwinfo' - Are you sure its installed?")
    hwinfos = [ el.strip() for el in p.stdout.decode().split("\n") ]
    return_dct['model'] = None
    return_dct['device'] = None
    for hwinfo in hwinfos:
        if hwinfo.startswith("Model:"): return_dct['model'] = hwinfo.split(":")[1].strip()
        if hwinfo.startswith("Device:"): return_dct['device'] = hwinfo.split(":")[1].strip()

    # get machine name
    machine_name = platform.node()
    return_dct['machine_name'] = machine_name

    # get cpu info
    processor_all = check_output("lscpu", shell=True).strip().decode()
    processor = "".join( [ ln.split(":")[1].strip() for ln in processor_all.split("\n") if ln.startswith("Model name:") ] )
    return_dct["processor"] = processor
    cpu_count = psutil.cpu_count(logical=True)
    return_dct["cpu_count"] = cpu_count
    cpu_freq = psutil.cpu_freq()
    return_dct["cpu_freq"] = str(cpu_freq)

    # get memory info
    mem_info = psutil.virtual_memory()
    return_dct['mem'] = str(mem_info)

    if verbose: print("returning source info=", return_dct)
    return return_dct

def get_leda_info( ):
    '''Get LedaG card info.'''

    slots = []

    #
    # This is gnarly code to invoke the ledagssh command,
    # async capture the output, detect the ledagssh prompt,
    # and send it the quit command, and capture any error
    # code when the process exits.
    #
    cmd = ledagssh.split()
    if verbose: print("\nRunning leda command", cmd, "\n" )
    p = Popen( cmd, stdout=PIPE, stdin=PIPE, stderr=STDOUT)
    os.set_blocking(p.stdout.fileno(), False)
    while True:
        if p.poll()!=None:
            if verbose: print("leda command terminated.")
            if p.returncode!=0:
                print("ERROR: Leda command returned error code %d" % p.returncode)
                return False
            else: break
        b = p.stdout.readline()
        if b==b'':
            time.sleep(0.01)
            continue
        bs = b.decode('utf-8')
        if verbose: print("leda output: %s" % bs, end="")
        if bs.find("slot")>=0:
            slots.append( bs )
        if bs.startswith("localhost >"):
            if verbose: print()
            p.communicate( str.encode("quit") )

    if verbose: print("ledag slot info:", slots)

    # return number of boards and the board slot details array
    return len(slots), slots


#
# config
#

# command to get LEDA card information
ledagssh    =   "ledag-ssh -o localhost"

# verbosity level
verbose = True

# whether or not to check for local file
require_local_path = True

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
pgvector_ver = '0.6.2'


# validate/get info on files
print("Getting file info for", DATA_PATH)
finfo = get_source_info(DATA_PATH)
print("file info=", finfo)


# form the data CSV save path
machine_name = platform.node()
save_path = './results/0416/pgvector_%s_%s_%d_%d_%d.csv'%(machine_name, basename, EFC, M, worker)
print("CSV save path=", save_path)


# connect to local postgres server
#conn = psycopg2.connect("host=localhost port=5432 dbname=postgres user=postgres password=gsi4ever target_session_attrs=read-write")
conn = psycopg.connect("host=localhost port=5432 dbname=postgres user=postgres password=gsi4ever target_session_attrs=read-write")
print(conn)

# Creating a cursor object
cursor = conn.cursor()

# create if doesnt exist
# enable extensions
# once per database
sql = "CREATE EXTENSION IF NOT EXISTS vector"
cursor.execute(sql)
print("created extensions")

register_vector(conn)

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
print(data.dtype, data.shape)

start_time = datetime.datetime.now()

print(f'Loading {len(data)} rows')
cur = conn.cursor()
    
with cur.copy('COPY test (embedding) FROM STDIN WITH (FORMAT BINARY)') as copy:
    copy.set_types(['vector'])

    for j, embedding in enumerate(data):
        # show progress
        if j % 10000 == 0:
            print('.', end='', flush=True)

        copy.write_row([embedding])

        # flush data
        while conn.pgconn.flush() == 1:
            pass


print('\nSuccess!')

add_time = datetime.datetime.now()
print("insert time: ", (add_time-start_time).total_seconds())


if args.stop_after_insert:
    print("stopping early..")
    try:
        conn.commit()
        conn.close()
    except:
        traceback.print_exc()
    sys.exit(0)

#####

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
        'walltime':(end_time-start_time).total_seconds(), 'insert_time': (add_time-start_time).total_seconds(),\
        'units':'seconds', 'dataset':basename, 'numrecs':num_records,\
        'dataset_finfo': finfo, 'pgvector_ver': pgvector_ver,\
        'ef_construction':EFC, 'M':M, 'ef_search':-1, 'labels':-1, 'distances':-1, \
        'memory':-1, 'workers':worker, 'buffer':buff})

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
                'dataset_finfo': finfo, 'pgvector_ver': pgvector_ver,\
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
