import psycopg2
import pandas as pd
import time
import io
import matplotlib.pyplot as plt

# Database Connection Settings
DB_CONFIG = {
    "dbname": "test",
    "user": "sumedhss",
    "password": "1234",
    "host": "localhost",
    "port": 5433
}

CSV_FILE = "github_events_m.csv"
ERROR_VALUES = [8,  16,  32, 64, 128,  256] # Powers of 4 starting near your range

def benchmark_fiting_error(error_val, bulk_df, insert_df):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    table_name = "test_fiting_throughput"
    index_name = "idx_fiting_param"
    
    # 1. Setup Table
    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    cur.execute(f"CREATE TABLE {table_name} (created_at TIMESTAMPTZ, date DATE, type TEXT, actor_login TEXT, event_id BIGINT);")
    
    # 2. 60% Bulk Load
    f = io.StringIO()
    bulk_df.to_csv(f, index=False, header=False)
    f.seek(0)
    cur.copy_from(f, table_name, sep=',', columns=('created_at', 'date', 'type', 'actor_login', 'event_id'))
    
    # 3. Create Index with Specific Error
    print(f"\rTesting Error Bound: {error_val}...", end="", flush=True)
    cur.execute(f"CREATE INDEX {index_name} ON {table_name} USING fiting (created_at);")
    cur.execute(f"ALTER INDEX {index_name} SET (max_error = {error_val}, btree_window_size = 8160);")
    cur.execute(f"REINDEX INDEX {index_name};")

    # 4. 10% Incremental Inserts (Measure Throughput)
    start_time = time.perf_counter()
    total_inserts = len(insert_df)
    
    for row in insert_df.itertuples(index=False):
        cur.execute(f"INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s);", 
                    (row.created_at, row.date, row.type, row.actor_login, row.event_id))
    
    end_time = time.perf_counter()
    total_time = end_time - start_time
    throughput = total_inserts / total_time # rows per second

    conn.close()
    return throughput

# --- DATA PREP ---
df = pd.read_csv(CSV_FILE).sample(frac=1).reset_index(drop=True)
bulk_data = df.iloc[:int(len(df) * 0.60)]
insert_data = df.iloc[int(len(df) * 0.60) : int(len(df) * 0.70)]

# --- EXECUTION ---
results = []
print("Starting FITing-Tree Parametric Benchmark...")
for err in ERROR_VALUES:
    tput = benchmark_fiting_error(err, bulk_data, insert_data)
    results.append(tput)
    print(f"\n   > Throughput for Error {err}: {tput:.2f} rows/sec")

# --- PLOTTING ---
plt.figure(figsize=(10, 6))
plt.plot(ERROR_VALUES, results, marker='o', linestyle='-', color='crimson', linewidth=2)

plt.xscale('log', base=4) # Log scale to match powers of 4
plt.title('FITing-Tree: Insert Throughput vs. Max Error Bound', fontsize=14)
plt.xlabel('Max Error (Log Scale Base 4)', fontsize=12)
plt.ylabel('Insert Throughput (Rows/Second)', fontsize=12)
plt.grid(True, which="both", ls="-", alpha=0.5)

plt.savefig('fiting_throughput_error.png')
print("\n" + "="*40)
print("Benchmark Complete. Plot saved as 'fiting_throughput_error.png'")
plt.show()