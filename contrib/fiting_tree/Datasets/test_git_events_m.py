import psycopg2
import pandas as pd
import time
import random
from datetime import datetime
import io

# Database Connection Settings
DB_CONFIG = {
    "dbname": "test",
    "user": "sumedhss",
    "password": "1234",
    "host": "localhost",
    "port": 5433
}

CSV_FILE = "github_events_m.csv" 

def run_test_suite(index_type, bulk_df, insert_df):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    
    table_name = f"test_gh_{index_type}"
    
    # --- 1. SETUP & BULK LOAD (80%) ---
    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    cur.execute(f"""
        CREATE TABLE {table_name} (
            created_at TIMESTAMPTZ,
            date DATE,
            type TEXT,
            actor_login TEXT,
            event_id BIGINT
        );
    """)
    
    print(f"Bulk loading 80% data for {index_type.upper()}...")
    
    # Convert DF to string buffer for high-speed COPY
    f = io.StringIO()
    bulk_df.to_csv(f, index=False, header=False)
    f.seek(0)
    
    cur.copy_from(f, table_name, sep=',', columns=('created_at', 'date', 'type', 'actor_login', 'event_id'))
    
    # Create Index
    index_name = f"idx_{index_type}"
    print(f"Creating {index_name}...")
    if index_type == "fiting":
        cur.execute(f"CREATE INDEX {index_name} ON {table_name} USING fiting (created_at);")
    else:
        cur.execute(f"CREATE INDEX {index_name} ON {table_name} USING btree (created_at);")

    # --- 2. SELECT QUERIES (50) ---
    # Pick 50 random timestamps from the already loaded data
    cur.execute(f"SELECT created_at FROM {table_name} ORDER BY random() LIMIT 50;")
    sample_keys = [r[0] for r in cur.fetchall()]

    select_times = []
    correct_count = 0
    for key in sample_keys:
        start = time.perf_counter()
        cur.execute(f"SELECT * FROM {table_name} WHERE created_at = %s;", (key,))
        res = cur.fetchall()
        select_times.append(time.perf_counter() - start)
        if len(res) > 0: correct_count += 1

    # --- 3. INCREMENTAL INSERTS (Remaining 20%) ---
    # This measures how the index handles actual data growth
    print(f"Performing incremental inserts (20% of dataset)...")
    insert_times = []
    
    # We iterate through the insert_df rows
    for row in insert_df.itertuples(index=False):
        start = time.perf_counter()
        cur.execute(f"""
            INSERT INTO {table_name} (created_at, date, type, actor_login, event_id) 
            VALUES (%s, %s, %s, %s, %s);
        """, (row.created_at, row.date, row.type, row.actor_login, row.event_id))
        insert_times.append(time.perf_counter() - start)

    # --- 4. MEMORY USAGE ---
    cur.execute(f"SELECT pg_relation_size('{index_name}');")
    index_size = cur.fetchone()[0]

    # --- 5. CLEANUP ---
    cur.execute(f"DROP TABLE {table_name} CASCADE;")
    conn.close()
    
    return {
        "avg_select": (sum(select_times) / 50) * 1000,
        "avg_insert": (sum(insert_times) / len(insert_times)) * 1000,
        "size_kb": index_size / 1024,
        "accuracy": (correct_count / 50) * 100
    }

# --- DATA PREPARATION ---
print("Reading and splitting dataset...")
df = pd.read_csv(CSV_FILE)
# Shuffle the data randomly
df = df.sample(frac=1).reset_index(drop=True)

# Split 80/20
split_idx = int(len(df) * 0.68)
last_idx = int(len(df) * 0.7)
bulk_data = df.iloc[:split_idx]
insert_data = df.iloc[split_idx:last_idx]

# --- EXECUTION ---
fiting_results = run_test_suite("fiting", bulk_data, insert_data)
btree_results = run_test_suite("btree", bulk_data, insert_data)

# --- REPORTING ---
print("\n" + "="*65)
print(f"{'Performance Metric':<25} | {'FITing-Tree':<15} | {'B-Tree':<15}")
print("-" * 65)
print(f"{'Avg Select Time (ms)':<25} | {fiting_results['avg_select']:<15.4f} | {btree_results['avg_select']:<15.4f}")
print(f"{'Avg Insert Time (ms)':<25} | {fiting_results['avg_insert']:<15.4f} | {btree_results['avg_insert']:<15.4f}")
print(f"{'Index Size (KB)':<25} | {fiting_results['size_kb']:<15.2f} | {btree_results['size_kb']:<15.2f}")
print(f"{'Query Accuracy (%)':<25} | {fiting_results['accuracy']:<15.1f} | {btree_results['accuracy']:<15.1f}")
print("="*65)