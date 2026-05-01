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

def get_index_size(index_type, data_subset):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    table_name = f"test_size_bench"
    index_name = f"idx_bench_{index_type}"
    
    # Clean and Rebuild Table
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
    
    # Bulk Load via COPY
    f = io.StringIO()
    data_subset.to_csv(f, index=False, header=False)
    f.seek(0)
    cur.copy_from(f, table_name, sep=',', columns=('created_at', 'date', 'type', 'actor_login', 'event_id'))
    
    # Create Index
    if index_type == "fiting":
        cur.execute(f"CREATE INDEX {index_name} ON {table_name} USING fiting (created_at);")
    else:
        cur.execute(f"CREATE INDEX {index_name} ON {table_name} USING btree (created_at);")

    # Measure Size
    cur.execute(f"SELECT pg_relation_size('{index_name}');")
    size_kb = cur.fetchone()[0] / 1024

    conn.close()
    return size_kb

# --- DATA PREPARATION ---
print("Reading dataset...")
df = pd.read_csv(CSV_FILE)
# Optional: Ensure data is sorted if FITing-Tree requires it for optimal construction
df = df.sort_values('created_at').reset_index(drop=True)

percentages = list(range(5, 101, 5))
btree_sizes = []
fiting_sizes = []
row_counts = []

# --- EXECUTION LOOP ---
for p in percentages:
    sample_size = int(len(df) * (p / 100))
    subset = df.iloc[:sample_size]
    row_counts.append(sample_size)
    
    print(f"Testing {p}% of data ({sample_size} rows)...")
    
    # Measure B-Tree
    b_size = get_index_size("btree", subset)
    btree_sizes.append(b_size)
    
    # Measure FITing-Tree
    f_size = get_index_size("fiting", subset)
    fiting_sizes.append(f_size)

# --- PLOTTING ---
plt.figure(figsize=(10, 6))
plt.plot(row_counts, btree_sizes, marker='o', linestyle='-', label='B-Tree', color='royalblue')
plt.plot(row_counts, fiting_sizes, marker='s', linestyle='--', label='FITing-Tree', color='crimson')

plt.title('Index Size Comparison: B-Tree vs FITing-Tree', fontsize=14)
plt.xlabel('Number of Rows (Data Size)', fontsize=12)
plt.ylabel('Index Size (KB)', fontsize=12)
plt.legend()
plt.grid(True, linestyle=':', alpha=0.6)
plt.tight_layout()

# Save and Show
plt.savefig('index_size_comparison.png')
print("\nPlot saved as 'index_size_comparison.png'")
plt.show()

# --- SUMMARY REPORT ---
print("\n" + "="*45)
print(f"{'% Data':<10} | {'B-Tree (KB)':<15} | {'FITing (KB)':<15}")
print("-" * 45)
for i, p in enumerate(percentages):
    print(f"{p:<10}% | {btree_sizes[i]:<15.2f} | {fiting_sizes[i]:<15.2f}")
print("="*45)