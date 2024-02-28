#!/bin/bash

# install 
sudo apt install postgresql
sudo apt install postgresql-server-dev-12

# install python support (recommend conda environment activated)
pip install psycopg2
python -c "import psycopg2" 
# NOTE: if above has error, locate your libffi.so.7 library and create a softlink to your conda lib directory here is an example:
# locate libffi.so.7:   
#   ldconfig -p |  grep ffi
# locate your conda lib directory:  
#   conda info --envs
# create soft link to the original libffi.so.7 within the conda lib environment- here is what we did on apu12
#   ln -sf /lib/x86_64-linux-gnu/libffi.so.7  /home/gwilliams/anaconda3/envs/pgvector_sabrina_fork/lib/libffi.so.7

echo "You should change the password of the user "postgres" within the psql console"
# NOTE: here is what we did:
# Enter psql console:
#  sudo -u postgres psql postgres
# Entered the command to change the password
#  postgres=# ALTER USER postgres WITH PASSWORD 'gsi4ever';
# Now your python program should be able to connect via psycopg2

