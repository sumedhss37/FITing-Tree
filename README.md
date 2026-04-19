FitingTree in PostgreSQL 
=====================================

This directory contains the implementation of FITing-Tree, a data aware learnable index in postgreSQL. 

To run the code, follow the below steps:

1) ./configure --enable-debug --without-icu
2) make 
3) sudo make install
4) make -C contrib/fiting_tree clean
5) make -C contrib/fiting_tree
6) sudo make -C contrib/fiting_tree install
7) /usr/local/pgsql/bin/initdb -D /tmp/pgdata
8) /usr/local/pgsql/bin/pg_ctl -D /tmp/pgdata -o "-p 5433" -l /tmp/pgdata/logfile start
9) /usr/local/pgsql/bin/psql -p 5433 -U harshul -d postgres


Sample commands:

DROP EXTENSION fiting_tree;
CREATE EXTENSION fiting_tree;

-- ---- int4 (existing behavior) ----
CREATE TABLE t_int (id int4);
INSERT INTO t_int SELECT generate_series(1, 10000);
CREATE INDEX ON t_int USING fiting (id);
SELECT * FROM t_int WHERE id = 5000;

-- ---- timestamp ----
CREATE TABLE t_ts (ts timestamp);
INSERT INTO t_ts
    SELECT '2020-01-01 00:00:00'::timestamp + (i * interval '1 second')
    FROM generate_series(1, 10000) AS i;
CREATE INDEX ON t_ts USING fiting (ts);

SELECT * FROM t_ts WHERE ts = '2020-01-01 01:23:45';   -- exact match
INSERT INTO t_ts VALUES ('2025-06-15 12:00:00');
SELECT * FROM t_ts WHERE ts = '2025-06-15 12:00:00';   -- buffer hit
DELETE FROM t_ts WHERE ts = '2020-01-01 00:00:01';
VACUUM t_ts;
SELECT * FROM t_ts WHERE ts = '2020-01-01 00:00:01';   -- deleted

-- ---- timestamptz ----
CREATE TABLE t_tstz (ts timestamptz);
INSERT INTO t_tstz
    SELECT '2020-01-01 00:00:00+00'::timestamptz + (i * interval '1 second')
    FROM generate_series(1, 10000) AS i;
CREATE INDEX ON t_tstz USING fiting (ts);
SELECT * FROM t_tstz WHERE ts = '2020-01-01 01:23:45+00';

CREATE INDEX ON t_ts USING btree (ts);
-- Compare index sizes
SELECT 
    indexrelid::regclass AS index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) AS size,
    pg_relation_size(indexrelid) AS bytes
FROM pg_index
WHERE indrelid = 't_ts'::regclass
ORDER BY bytes DESC;

The idea has been taken from : 
Alex Galakatos, Michael Markovitch, Carsten Binnig, Rodrigo Fonseca, and Tim Kraska. 2019. FITing-Tree: A Data-aware Index Structure. In Proceedings of the 2019 International Conference on Management of Data (SIGMOD '19). Association for Computing Machinery, New York, NY, USA, 1189–1206. https://doi.org/10.1145/3299869.3319860

