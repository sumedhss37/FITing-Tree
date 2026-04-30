#!/usr/bin/env python3
"""
bench_demo.py
=============
B-Tree vs FITing-Tree — parallel tracking across every lifecycle phase.

Schema:  demo(x int4, y int4)   where  y ≈ 5·x + 10  (+ small uniform noise)

Lifecycle
---------
  0  Create table, seed 100 000 rows
  1  Create BOTH indices (B-Tree + FITing)  →  snapshot sizes
  2  CLUSTER heap on B-Tree                →  snapshot sizes
  3  EXPLAIN ANALYZE (both indices)
  4  Bulk INSERT 100 000 rows              →  snapshot sizes
  5  Bulk DELETE 100 000 rows              →  snapshot sizes
  6  VACUUM ANALYZE                        →  snapshot sizes
  7  Post-mutation EXPLAIN ANALYZE (both)
  8  Correctness check
  9  Summary table
 10  Cleanup
"""

import psycopg2
import time
import sys

# ── tunables ─────────────────────────────────────────────────────────────────
DSN         = "host=localhost dbname=test user=sumedhss password=1234 port=5433"
SEED_ROWS   = 50_000
EXTRA_ROWS  = 50_000
DELETE_ROWS = 50_000
BATCH_SIZE  =   5_000
SEARCH_Y    =  50_000
# ─────────────────────────────────────────────────────────────────────────────

# Snapshot store: list of dicts, one per milestone
snapshots: list[dict] = []


def connect() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    return conn


def hr(title: str = "") -> None:
    width = 66
    if title:
        pad  = (width - len(title) - 2) // 2
        line = "─" * pad + f" {title} " + "─" * max(0, width - pad - len(title) - 2)
    else:
        line = "─" * width
    print(f"\n{line}")


def get_sizes(cur) -> tuple[int, int]:
    """Return (btree_bytes, fiting_bytes). Returns 0 if index doesn't exist."""
    def sz(name):
        cur.execute("""
            SELECT COALESCE(pg_relation_size(c.oid), 0)
            FROM   pg_class c
            WHERE  c.relname = %s AND c.relkind = 'i'
        """, (name,))
        row = cur.fetchone()
        return row[0] if row else 0
    return sz('demo_btree_idx'), sz('demo_fiting_idx')


def snap(conn, label: str) -> None:
    """Record a size snapshot and print an inline report."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM demo")
        rows = cur.fetchone()[0]
        bt, ft = get_sizes(cur)

    entry = dict(label=label, rows=rows, btree=bt, fiting=ft)
    snapshots.append(entry)

    def fmt(b):
        if b == 0:
            return "—"
        kb = b / 1024
        if kb >= 1024:
            return f"{kb/1024:.1f} MB  ({b:,} B)"
        return f"{kb:.0f} kB  ({b:,} B)"

    print(f"  rows       : {rows:,}")
    print(f"  B-Tree     : {fmt(bt)}")
    print(f"  FITing     : {fmt(ft)}")


def explain_both(conn, label_prefix: str) -> None:
    """Run EXPLAIN ANALYZE with each index forced, print both plans."""
    query = f"SELECT * FROM demo WHERE y = {SEARCH_Y}"

    # Force B-Tree (bitmapscan=off so it uses plain indexscan)
    with conn.cursor() as cur:
        cur.execute("SET enable_seqscan    = off")
        cur.execute("SET enable_bitmapscan = off")
        cur.execute("SET enable_indexscan  = on")
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {query}")
        plan_bt = cur.fetchall()
        cur.execute("RESET ALL")

    hr(f"{label_prefix} — B-Tree plan  (y = {SEARCH_Y:,})")
    for (line,) in plan_bt:
        print(line)

    # Force FITing (drop btree temporarily so planner has no choice)
    with conn.cursor() as cur:
        cur.execute("SET enable_seqscan    = off")
        cur.execute("SET enable_bitmapscan = off")
        cur.execute("SET enable_indexscan  = on")
        # Disable b-tree so planner picks fiting
        cur.execute("UPDATE pg_index SET indisvalid = false "
                    "WHERE indexrelid = 'demo_btree_idx'::regclass")
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {query}")
        plan_ft = cur.fetchall()
        cur.execute("UPDATE pg_index SET indisvalid = true "
                    "WHERE indexrelid = 'demo_btree_idx'::regclass")
        cur.execute("RESET ALL")

    hr(f"{label_prefix} — FITing plan  (y = {SEARCH_Y:,})")
    for (line,) in plan_ft:
        print(line)


# ── Phase 0 : seed ────────────────────────────────────────────────────────────
def phase_seed(conn) -> None:
    hr("Phase 0 — create table + seed 100 000 rows")
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS demo CASCADE")
        cur.execute("CREATE TABLE demo (x int4, y int4)")

        print(f"Inserting {SEED_ROWS:,} seed rows …")
        t0 = time.perf_counter()
        cur.execute("""
            INSERT INTO demo (x, y)
            SELECT i,
                   (5 * i + 10 + (random() - 0.5) * 10)::int4
            FROM generate_series(1, %s) i
        """, (SEED_ROWS,))
        print(f"  done in {time.perf_counter()-t0:.2f}s")
        print(f"  y range : ≈15 … ≈{5*SEED_ROWS+10:,}")
        print(f"  probe y={SEARCH_Y:,}  →  x ≈ {(SEARCH_Y-10)//5:,}")


# ── Phase 1 : create both indices ────────────────────────────────────────────
def phase_create_indices(conn) -> None:
    hr("Phase 1 — create B-Tree + FITing (seed rows only)")
    with conn.cursor() as cur:
        print("Creating B-Tree index …")
        t0 = time.perf_counter()
        cur.execute("CREATE INDEX demo_btree_idx ON demo (y)")
        print(f"  done in {time.perf_counter()-t0:.2f}s")

        print("Creating FITing index …")
        t0 = time.perf_counter()
        cur.execute("CREATE INDEX demo_fiting_idx ON demo USING fiting (y)")
        print(f"  done in {time.perf_counter()-t0:.2f}s")

    snap(conn, "After bulk load (100k rows)")


# ── Phase 2 : cluster ─────────────────────────────────────────────────────────
def phase_cluster(conn) -> None:
    hr("Phase 2 — CLUSTER heap on B-Tree")
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute("CLUSTER demo USING demo_btree_idx")
        print(f"  CLUSTER done in {time.perf_counter()-t0:.2f}s")
        print("  (FITing index is rebuilt automatically after CLUSTER)")

    snap(conn, "After CLUSTER")


# ── Phase 3 : EXPLAIN ANALYZE (baseline) ─────────────────────────────────────
def phase_explain_baseline(conn) -> None:
    explain_both(conn, "Baseline")


# ── Phase 4 : bulk INSERT ─────────────────────────────────────────────────────
def phase_bulk_insert(conn) -> None:
    hr("Phase 4 — Bulk INSERT 100 000 rows")
    start_x = SEED_ROWS + 1
    end_x   = SEED_ROWS + EXTRA_ROWS
    print(f"  x range : {start_x:,} … {end_x:,}   "
          f"y range : ≈{5*start_x+10:,} … ≈{5*end_x+10:,}")
    print(f"  inserting in batches of {BATCH_SIZE:,} …")

    t0      = time.perf_counter()
    batches = 0
    with conn.cursor() as cur:
        for lo in range(start_x, end_x + 1, BATCH_SIZE):
            hi = min(lo + BATCH_SIZE - 1, end_x)
            cur.execute("""
                INSERT INTO demo (x, y)
                SELECT i,
                       (5 * i + 10 + (random() - 0.5) * 10)::int4
                FROM generate_series(%s, %s) i
            """, (lo, hi))
            batches += 1

    print(f"  {batches} batches done in {time.perf_counter()-t0:.2f}s")
    snap(conn, "After bulk INSERT (+100k rows)")


# ── Phase 5 : bulk DELETE ─────────────────────────────────────────────────────
def phase_bulk_delete(conn) -> None:
    hr("Phase 5 — Bulk DELETE 100 000 rows")
    start_x = SEED_ROWS + 1
    end_x   = SEED_ROWS + DELETE_ROWS
    print(f"  deleting x = {start_x:,} … {end_x:,} in batches of {BATCH_SIZE:,} …")

    t0      = time.perf_counter()
    batches = 0
    with conn.cursor() as cur:
        for lo in range(start_x, end_x + 1, BATCH_SIZE):
            hi = min(lo + BATCH_SIZE - 1, end_x)
            cur.execute("DELETE FROM demo WHERE x BETWEEN %s AND %s", (lo, hi))
            batches += 1

    print(f"  {batches} batches done in {time.perf_counter()-t0:.2f}s")
    print("  (FITing tombstones still live; B-Tree dead tuples still present)")
    snap(conn, "After bulk DELETE (pre-VACUUM)")


# ── Phase 6 : VACUUM ──────────────────────────────────────────────────────────
def phase_vacuum(conn) -> None:
    hr("Phase 6 — VACUUM ANALYZE")
    with conn.cursor() as cur:
        print("Running VACUUM ANALYZE …")
        t0 = time.perf_counter()
        cur.execute("VACUUM ANALYZE demo")
        print(f"  done in {time.perf_counter()-t0:.2f}s")

    snap(conn, "After VACUUM")


# ── Phase 7 : EXPLAIN ANALYZE (post-mutation) ─────────────────────────────────
def phase_explain_post(conn) -> None:
    explain_both(conn, "Post-mutation")


# ── Phase 8 : correctness ─────────────────────────────────────────────────────
def phase_verify(conn) -> None:
    hr("Phase 8 — correctness check")
    with conn.cursor() as cur:
        cur.execute("SET enable_indexscan = off; SET enable_bitmapscan = off")
        cur.execute(f"SELECT count(*) FROM demo WHERE y = {SEARCH_Y}")
        heap_n = cur.fetchone()[0]

        cur.execute("SET enable_indexscan = on; SET enable_seqscan = off; "
                    "SET enable_bitmapscan = off")
        cur.execute(f"SELECT count(*) FROM demo WHERE y = {SEARCH_Y}")
        idx_n = cur.fetchone()[0]

        cur.execute("RESET ALL")

    ok = heap_n == idx_n
    print(f"  y = {SEARCH_Y:,}")
    print(f"  Heap (seqscan)  : {heap_n}")
    print(f"  FITing index    : {idx_n}")
    print(f"  {'✓  MATCH' if ok else '✗  MISMATCH — investigate!'}")


# ── Phase 9 : summary table ───────────────────────────────────────────────────
def phase_summary() -> None:
    hr("Phase 9 — index size summary")

    def fmt_kb(b: int) -> str:
        if b == 0:
            return "—"
        return f"{b/1024:>8.0f} kB"

    def fmt_b(b: int) -> str:
        if b == 0:
            return "—"
        return f"{b:>12,} B"

    col_w   = 26
    num_w   = 11
    bytes_w = 14

    hdr = (f"  {'Phase':<{col_w}}"
           f"  {'B-Tree':>{num_w}}  {'':>{bytes_w}}"
           f"  {'FITing':>{num_w}}  {'':>{bytes_w}}"
           f"  {'Rows':>10}")
    sep = "  " + "─" * (col_w + 2*(num_w + bytes_w + 4) + 12)

    print(f"\n  {'Phase':<{col_w}}"
          f"  {'B-Tree kB':>{num_w}}  {'B-Tree bytes':>{bytes_w}}"
          f"  {'FITing kB':>{num_w}}  {'FITing bytes':>{bytes_w}}"
          f"  {'Rows':>10}")
    print(sep)

    for s in snapshots:
        bt = s['btree']
        ft = s['fiting']
        print(f"  {s['label']:<{col_w}}"
              f"  {fmt_kb(bt):>{num_w}}  {fmt_b(bt):>{bytes_w}}"
              f"  {fmt_kb(ft):>{num_w}}  {fmt_b(ft):>{bytes_w}}"
              f"  {s['rows']:>10,}")

    print(sep)

    # ratio row (last snapshot)
    last = snapshots[-1]
    bt, ft = last['btree'], last['fiting']
    if bt > 0 and ft > 0:
        print(f"\n  FITing / B-Tree ratio (final state) : {ft/bt:.2f}x")


# ── Phase 10 : cleanup ────────────────────────────────────────────────────────
def phase_cleanup(conn) -> None:
    hr("Phase 10 — cleanup")
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS demo CASCADE")
    print("  Table demo (+ all indices) dropped.")


# ── main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    print("bench_demo.py — B-Tree vs FITing-Tree lifecycle comparison")
    print(f"  y = 5·x + 10 + noise(±5)  →  linear data, ideal for learned index")
    print(f"  seed rows   : {SEED_ROWS:,}")
    print(f"  extra rows  : {EXTRA_ROWS:,}  (inserted then deleted)")
    print(f"  batch size  : {BATCH_SIZE:,}")
    print(f"  probe  y    : {SEARCH_Y:,}")

    try:
        conn = connect()
    except Exception as exc:
        print(f"Connection failed: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        phase_seed(conn)
        phase_create_indices(conn)
        phase_cluster(conn)
        phase_explain_baseline(conn)
        phase_bulk_insert(conn)
        phase_bulk_delete(conn)
        phase_vacuum(conn)
        phase_explain_post(conn)
        phase_verify(conn)
        phase_summary()
        phase_cleanup(conn)
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        with conn.cursor() as cur:
            try:
                cur.execute("DROP TABLE IF EXISTS demo CASCADE")
            except Exception:
                pass
        sys.exit(1)
    finally:
        conn.close()

    hr()
    print("Done.")


if __name__ == "__main__":
    main()
