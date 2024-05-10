SET client_min_messages = DEBUG;
SHOW client_min_messages;
SELECT * fROM pg_extension;

select count(*) from test; 

SET max_parallel_maintenance_workers = 32;
SET max_parallel_workers = 32;
-- value 100GB, change to 250GB for 250M
SET maintenance_work_mem = '250GB';

show max_parallel_maintenance_workers;
show max_parallel_workers;
show maintenance_work_mem;
show shared_buffers;
show data_directory;

select count(*) from pg_indexes where tablename = 'test';

select now();
CREATE INDEX concurrently ON test USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);
select now();

select count(*) from pg_indexes where tablename = 'test';


