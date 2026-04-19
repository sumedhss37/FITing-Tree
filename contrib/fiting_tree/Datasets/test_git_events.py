import psycopg2
import pandas as pd
import time
import random
from datetime import datetime

# Database Connection Settings
DB_CONFIG = {
    "dbname": "test",
    "user": "sumedhss",
    "password": "1234",
    "host": "localhost",
    "port": 5433
}

CSV_FILE = "github_events.csv" 

def run_test_suite(index_type="fiting"):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    
    table_name = f"test_gh_{index_type}"
    
    # --- 1. SETUP & BULK LOAD ---
    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    cur.execute(f"""
        CREATE TABLE {table_name} (
            id BIGINT,
            type TEXT,
            actor_login TEXT,
            repo_name TEXT,
            payload_action TEXT,
            created_at TIMESTAMPTZ
        );
    """)
    
    print(f"Loading data for {index_type.upper()}...")
    with open(CSV_FILE, 'r') as f:
        next(f) # Skip CSV header
        cur.copy_from(f, table_name, sep=',', columns=('id', 'type', 'actor_login', 'repo_name', 'payload_action', 'created_at'))
    
    # Create Index
    index_name = f"idx_{index_type}"
    if index_type == "fiting":
        cur.execute(f"CREATE INDEX {index_name} ON {table_name} USING fiting (created_at);")
    else:
        cur.execute(f"CREATE INDEX {index_name} ON {table_name} USING btree (created_at);")

    # Get sample keys for queries
    cur.execute(f"SELECT created_at FROM {table_name} ORDER BY random() LIMIT 50;")
    sample_keys = [r[0] for r in cur.fetchall()]

    # --- 2. SELECT QUERIES (50) ---
    select_times = []
    correct_count = 0
    for key in sample_keys:
        start = time.time()
        cur.execute(f"SELECT * FROM {table_name} WHERE created_at = %s;", (key,))
        res = cur.fetchall()
        select_times.append(time.time() - start)
        if len(res) > 0: correct_count += 1

    # --- 3. RANDOM INSERTS (500) ---
    insert_times = []
    for _ in range(500):
        # Insert a "future" timestamp
        new_ts = datetime(2025, 6, 15, random.randint(0,23), random.randint(0,59))
        start = time.time()
        cur.execute(f"INSERT INTO {table_name} (id, created_at) VALUES (%s, %s);", (random.getrandbits(31), new_ts))
        insert_times.append(time.time() - start)

    # --- 4. RANDOM DELETES (500) ---
    # We grab 500 existing IDs to delete
    cur.execute(f"SELECT id FROM {table_name} ORDER BY random() LIMIT 500;")
    ids_to_delete = [r[0] for r in cur.fetchall()]
    
    delete_times = []
    for d_id in ids_to_delete:
        start = time.time()
        cur.execute(f"DELETE FROM {table_name} WHERE id = %s;", (d_id,))
        delete_times.append(time.time() - start)

    # --- 5. MEMORY USAGE ---
    cur.execute(f"SELECT pg_relation_size('{index_name}');")
    index_size = cur.fetchone()[0]

    # --- 6. DROP TABLE (Cleanup) ---
    cur.execute(f"DROP TABLE {table_name} CASCADE;")
    
    conn.close()
    
    return {
        "avg_select": (sum(select_times) / 50) * 1000,
        "avg_write": ((sum(insert_times) + sum(delete_times)) / 1000) * 1000,
        "size_kb": index_size / 1024,
        "accuracy": (correct_count / 50) * 100
    }

# Execute comparison
fiting = run_test_suite("fiting")
btree = run_test_suite("btree")

# --- 7. REPORTING ---
print("\n" + "="*60)
print(f"{'Performance Metric':<25} | {'FITing-Tree':<15} | {'B-Tree':<15}")
print("-" * 60)
print(f"{'Avg Select Time (ms)':<25} | {fiting['avg_select']:<15.4f} | {btree['avg_select']:<15.4f}")
print(f"{'Avg Write Time (ms)':<25} | {fiting['avg_write']:<15.4f} | {btree['avg_write']:<15.4f}")
print(f"{'Index Size (KB)':<25} | {fiting['size_kb']:<15.2f} | {btree['size_kb']:<15.2f}")
print(f"{'Query Accuracy (%)':<25} | {fiting['accuracy']:<15.1f} | {btree['accuracy']:<15.1f}")
print("="*60)
print("All temporary tables and indexes have been dropped.")