FitingTree in PostgreSQL 
=====================================

This directory contains the implementation of FITing-Tree, a data aware learnable index in postgreSQL. 

To run the code, follow the below steps:

1) ./configure --enable-debug --without-icu
2) make 
3) sudo amke install
4) make -C contrib/fiting_tree clean
5) make -C contrib/fiting_tree
6) sudo make -C contrib/fiting_tree install
7) /usr/local/pgsql/bin/initdb -D /tmp/pgdata
8) /usr/local/pgsql/bin/pg_ctl -D /tmp/pgdata -o "-p 5433" -l /tmp/pgdata/logfile start
9) /usr/local/pgsql/bin/psql -p 5433 -U harshul -d postgres

Sample commands for testing:

CREATE EXTENSION IF NOT EXISTS fiting_tree;

CREATE TABLE demo (k int4, v text);
INSERT INTO demo SELECT i, 'val'||i FROM generate_series(1,1000) i;

CREATE INDEX demo_tmp ON demo(k);
CLUSTER demo USING demo_tmp;
DROP INDEX demo_tmp;

CREATE INDEX demo_fiting ON demo USING fiting (k);  

SET enable_seqscan = off;
SET enable_bitmapscan = off;

EXPLAIN (COSTS OFF) SELECT * FROM demo WHERE k = 500;
SELECT * FROM demo WHERE k = 500;
SELECT * FROM demo WHERE k = 1;
SELECT * FROM demo WHERE k = 1000;
SELECT * FROM demo WHERE k = 9999;   -- should return 0 rows

RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE demo;

The idea has been taken from : 
Alex Galakatos, Michael Markovitch, Carsten Binnig, Rodrigo Fonseca, and Tim Kraska. 2019. FITing-Tree: A Data-aware Index Structure. In Proceedings of the 2019 International Conference on Management of Data (SIGMOD '19). Association for Computing Machinery, New York, NY, USA, 1189–1206. https://doi.org/10.1145/3299869.3319860

