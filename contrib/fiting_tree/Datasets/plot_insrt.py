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
MEASURE_INTERVAL = 25  # Record index size every 100 inserts

def run_dynamic_test(index_type, bulk_df, incremental_df):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    table_name = f"test_dynamic_{index_type}"
    index_name = f"idx_dyn_{index_type}"
    
    # --- 1. CLEANUP & TABLE CREATION ---
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
    
    # --- 2. SEED DATA (10% BULK LOAD) ---
    print(f"\n[{index_type.upper()}] Step 1: Bulk loading 10% seed data...")
    f = io.StringIO()
    bulk_df.to_csv(f, index=False, header=False)
    f.seek(0)
    cur.copy_from(f, table_name, sep=',', columns=('created_at', 'date', 'type', 'actor_login', 'event_id'))
    
    # Create the specific index
    if index_type == "fiting":
        cur.execute(f"CREATE INDEX {index_name} ON {table_name} USING fiting (created_at);")
    else:
        cur.execute(f"CREATE INDEX {index_name} ON {table_name} USING btree (created_at);")

    # --- 3. DYNAMIC INSERTS (60% ONE-BY-ONE) ---
    sizes = []
    iterations = []
    total_to_insert = len(incremental_df)
    
    print(f"[{index_type.upper()}] Step 2: Performing {total_to_insert} incremental inserts...")
    
    for i, row in enumerate(incremental_df.itertuples(index=False), 1):
        cur.execute(f"""
            INSERT INTO {table_name} (created_at, date, type, actor_login, event_id) 
            VALUES (%s, %s, %s, %s, %s);
        """, (row.created_at, row.date, row.type, row.actor_login, row.event_id))
        
        # Carriage Return Progress Tracker
        if i % 10 == 0 or i == total_to_insert:
            print(f"\r   > Progress: {i}/{total_to_insert} inserts processed...", end="", flush=True)

        # Measure size at intervals
        if i % MEASURE_INTERVAL == 0 or i == total_to_insert:
            cur.execute(f"SELECT pg_relation_size('{index_name}');")
            size_kb = cur.fetchone()[0] / 1024
            sizes.append(size_kb)
            iterations.append(i)

    print(f"\n[{index_type.upper()}] Completed. Final Size: {sizes[-1]:.2f} KB")
    conn.close()
    return iterations, sizes

# --- MAIN EXECUTION ---
print("Reading and shuffling dataset...")
df = pd.read_csv(CSV_FILE)
# Shuffle to ensure random insertion order
df = df.sample(frac=1).reset_index(drop=True)

# Define splits
n = len(df)
bulk_data = df.iloc[:int(n * 0.50)]
incremental_data = df.iloc[int(n * 0.50) : int(n * 0.70)]

# Run tests
iters_f, sizes_f = run_dynamic_test("fiting", bulk_data, incremental_data)
iters_b, sizes_b = run_dynamic_test("btree", bulk_data, incremental_data)

# --- VISUALIZATION ---
plt.figure(figsize=(12, 7))

# Plot B-Tree
plt.plot(iters_b, sizes_b, label='B-Tree', color='#1f77b4', linewidth=2.5)

# Plot FITing-Tree
plt.plot(iters_f, sizes_f, label='FITing-Tree', color='#d62728', linewidth=2.5, linestyle='--')

# Formatting
plt.title('Index Size Growth: B-Tree vs. FITing-Tree (Incremental Workload)', fontsize=14, fontweight='bold')
plt.xlabel(f'Number of Incremental Inserts (Seed: {len(bulk_data)} rows)', fontsize=12)
plt.ylabel('Index Size (KB)', fontsize=12)
plt.legend(loc='upper left', frameon=True, shadow=True)
plt.grid(True, linestyle=':', alpha=0.7)

# Add text annotations for final size comparison
plt.annotate(f'Final: {sizes_b[-1]:.1f}KB', xy=(iters_b[-1], sizes_b[-1]), xytext=(5, 5), 
             textcoords='offset points', color='#1f77b4', fontweight='bold')
plt.annotate(f'Final: {sizes_f[-1]:.1f}KB', xy=(iters_f[-1], sizes_f[-1]), xytext=(5, -15), 
             textcoords='offset points', color='#d62728', fontweight='bold')

plt.tight_layout()
plt.savefig('growth_comparison_dynamic.png', dpi=300)
print("\n" + "="*50)
print("Benchmarking Complete!")
print("Plot saved as: growth_comparison_dynamic.png")
print("="*50)
plt.show()