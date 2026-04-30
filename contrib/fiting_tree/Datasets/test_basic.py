"""
simple_test.py — FITing-Tree correctness test with unique keys.

Every inserted row gets its own distinct timestamp (BASE + i seconds),
so there is exactly one matching row per key.  

count_index() disables seqscan so the FITing index is actually used.
count_heap()  disables index scans  so the heap is read directly.
Both must agree at every checkpoint.
"""

import psycopg2
from datetime import datetime, timezone, timedelta

# ── connection ────────────────────────────────────────────────────────────────
DB = dict(dbname="test", user="sumedhss", password="1234",
          host="localhost", port=5433)

TABLE = "ft_simple_test"
IDX   = "ft_simple_idx"

# Each test row is inserted at BASE_TS + i seconds  (i = 1, 2, 3, …)
BASE_TS = datetime(2015, 1, 1, 15, 26, 40, tzinfo=timezone.utc)

# Seed timestamps: spread across the full dataset range so ShrinkingCone
# produces at least one real segment before we start inserting.
SEED_TS = [
    datetime(2015, 1, 1, h, m, s, tzinfo=timezone.utc)
    for h, m, s in [
        (15,  0,  0),
        (15, 10,  0),
        (15, 20,  0),
        (15, 26, 40),   # same minute as BASE_TS — seed gives baseline = 1
        (15, 40,  0),
        (15, 53,  0),
    ]
]

N_INSERT = 35   # 35 > max_error(32) → resegment fires during this batch
N_DELETE = 10   # delete the first N_DELETE of the inserted rows

# ── helpers ───────────────────────────────────────────────────────────────────

def kb(cur, rel):
    cur.execute("SELECT pg_relation_size(%s)", (rel,))
    return cur.fetchone()[0] // 1024

def sizes(cur):
    return f"index={kb(cur, IDX):3d} KB  table={kb(cur, TABLE):3d} KB"

def count_index(cur, ts):
    """Lookup via FITing index (seqscan disabled so index must be used)."""
    cur.execute("SET enable_seqscan = off")
    cur.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE created_at = %s", (ts,))
    n = cur.fetchone()[0]
    cur.execute("SET enable_seqscan = on")
    return n

def count_heap(cur, ts):
    """Lookup via heap seqscan (index disabled)."""
    cur.execute("SET enable_indexscan  = off")
    cur.execute("SET enable_bitmapscan = off")
    cur.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE created_at = %s", (ts,))
    n = cur.fetchone()[0]
    cur.execute("SET enable_indexscan  = on")
    cur.execute("SET enable_bitmapscan = on")
    return n

def verify(label, cur, timestamps, expect):
    """Check every timestamp: index and seqscan must both equal expect."""
    passed = failed = 0
    for ts in timestamps:
        idx = count_index(cur, ts)
        seq = count_heap(cur, ts)
        if idx == seq == expect:
            passed += 1
        else:
            failed += 1
            print(f"    FAIL  {ts.strftime('%H:%M:%S')}  "
                  f"index={idx}  seqscan={seq}  want={expect}")
    status = "ALL PASS" if failed == 0 else f"{failed} FAILED"
    print(f"  {label}: {passed}/{passed+failed} rows correct  [{status}]")

# ── setup ─────────────────────────────────────────────────────────────────────
conn = psycopg2.connect(**DB)
conn.autocommit = True
cur  = conn.cursor()

cur.execute(f"DROP TABLE IF EXISTS {TABLE} CASCADE")
cur.execute(f"""
    CREATE TABLE {TABLE} (
        row_id     BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ
    )
""")

# Insert seed rows BEFORE building the index so ShrinkingCone has data.
for ts in SEED_TS:
    cur.execute(f"INSERT INTO {TABLE} (created_at) VALUES (%s)", (ts,))

cur.execute(f"CREATE INDEX {IDX} ON {TABLE} USING fiting (created_at)")
print(f"Seed: {len(SEED_TS)} rows inserted, FITing index built.")
print(f"  {sizes(cur)}\n")

# ── baseline: seed rows findable ──────────────────────────────────────────────
print("=== BASELINE ===")
verify("seed timestamps", cur, SEED_TS, expect=1)
print(f"  {sizes(cur)}\n")

# ── insert N_INSERT rows at unique timestamps ─────────────────────────────────
# Timestamps: BASE_TS+1s, BASE_TS+2s, … BASE_TS+N_INSERTs
# All fall in the same segment range → 32nd insert triggers fiting_resegment.
inserted_ts  = [BASE_TS + timedelta(seconds=i + 1) for i in range(N_INSERT)]
inserted_ids = []
for ts in inserted_ts:
    cur.execute(
        f"INSERT INTO {TABLE} (created_at) VALUES (%s) RETURNING row_id", (ts,))
    inserted_ids.append(cur.fetchone()[0])

print(f"=== AFTER {N_INSERT} INSERTS  "
      f"(BASE+1s … BASE+{N_INSERT}s, resegment fires at 32) ===")
verify("inserted timestamps", cur, inserted_ts, expect=1)
print(f"  {sizes(cur)}\n")

# ── delete first N_DELETE inserted rows ───────────────────────────────────────
cur.execute(
    f"DELETE FROM {TABLE} WHERE row_id = ANY(%s)", (inserted_ids[:N_DELETE],))

deleted_ts   = inserted_ts[:N_DELETE]
surviving_ts = inserted_ts[N_DELETE:]

print(f"=== AFTER DELETING {N_DELETE} ROWS ===")
verify("deleted rows  (expect 0)", cur, deleted_ts,   expect=0)
verify("surviving rows(expect 1)", cur, surviving_ts, expect=1)
print(f"  {sizes(cur)}\n")

# ── seed rows must be completely unaffected ────────────────────────────────────
print("=== SEED ROWS UNCHANGED ===")
verify("seed timestamps", cur, SEED_TS, expect=1)
print(f"  {sizes(cur)}\n")

# ── cleanup ───────────────────────────────────────────────────────────────────
cur.execute(f"DROP TABLE {TABLE} CASCADE")
conn.close()
print("Done.")
