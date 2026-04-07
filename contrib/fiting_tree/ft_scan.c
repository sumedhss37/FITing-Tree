/*-------------------------------------------------------------------------
 *
 * ft_scan.c
 *   FITing-Tree index scan (ambeginscan / amrescan / amgettuple / amendscan).
 *
 * Lookup algorithm for a single equality key (Checkpoint 1):
 *
 *   1. Read meta page (block 0) → get total_tuples, max_error, num_segments.
 *   2. Read directory page (block 1) → copy FitingDirEntry array.
 *   3. Binary-search the directory for the segment whose start_key is the
 *      largest start_key ≤ search_key.
 *   4. Predict the rank:
 *        predicted = seg.base_rank + (search_key − seg.start_key) × seg.slope
 *   5. Clamp to [0, total_tuples−1] and set the search window:
 *        lo = max(0, predicted − max_error)
 *        hi = min(total_tuples−1, predicted + max_error)
 *   6. Binary-search the sorted leaf pages within [lo, hi] for search_key.
 *   7. If found, set scan->xs_heaptid and return true; otherwise return false.
 *
 * The ShrinkingCone guarantee ensures the actual rank is within max_error of
 * predicted, so the binary search window always contains the key if it exists.
 *
 * Assumptions (Checkpoint 1):
 *   - Equality scans only (strategy number 1 in the fiting_int4_ops opclass).
 *   - Keys are int4.
 *   - At most max_error duplicate entries per key value.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relscan.h"
#include "fiting_tree.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/* -----------------------------------------------------------------------
 * get_leaf_tuple_at_rank
 *
 * Read the FitingLeafTuple at position `rank` in the sorted data array.
 * Returns false if rank is out of bounds.
 * ----------------------------------------------------------------------- */
static bool
get_leaf_tuple_at_rank(Relation index, int64 rank, int64 total_tuples,
					   FitingLeafTuple *out)
{
	BlockNumber		blkno;
	int32			slot;
	Buffer			buf;
	Page			page;
	FitingLeafTuple *tuples;

	if (rank < 0 || rank >= total_tuples)
		return false;

	blkno = FITING_DATA_START_BLKNO + (BlockNumber) (rank / FITING_TUPLES_PER_PAGE);
	slot  = (int32) (rank % FITING_TUPLES_PER_PAGE);

	buf = ReadBuffer(index, blkno);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	tuples = FitingPageGetLeaf(page);
	*out = tuples[slot];

	UnlockReleaseBuffer(buf);
	return true;
}

/* -----------------------------------------------------------------------
 * find_segment_index
 *
 * Binary-search the directory for the largest i where dir[i].start_key ≤ key.
 * Returns -1 if key < dir[0].start_key (key is before the entire index).
 * ----------------------------------------------------------------------- */
static int
find_segment_index(const FitingDirEntry *dir, int num_segs, int32 key)
{
	int lo = 0, hi = num_segs - 1, result = -1;

	while (lo <= hi)
	{
		int mid = (lo + hi) / 2;

		if (dir[mid].start_key <= key)
		{
			result = mid;
			lo = mid + 1;
		}
		else
		{
			hi = mid - 1;
		}
	}
	return result;
}

/* -----------------------------------------------------------------------
 * fiting_beginscan
 * ----------------------------------------------------------------------- */
IndexScanDesc
fiting_beginscan(Relation indexRelation, int nkeys, int norderbys)
{
	IndexScanDesc	scan;
	FitingScanOpaque so;

	scan = RelationGetIndexScan(indexRelation, nkeys, norderbys);

	so = palloc0(sizeof(FitingScanOpaqueData));
	so->done = false;
	so->search_key = 0;

	scan->opaque = so;
	return scan;
}

/* -----------------------------------------------------------------------
 * fiting_rescan
 *
 * Called before each use of the index scan (including the first time).
 * Copies the scan keys and resets scan state.
 * ----------------------------------------------------------------------- */
void
fiting_rescan(IndexScanDesc scan, ScanKey keys, int nkeys,
			  ScanKey orderbys, int norderbys)
{
	FitingScanOpaque so = (FitingScanOpaque) scan->opaque;

	/* Reset for new search */
	so->done = false;

	/* Copy keys into scan->keyData (standard PostgreSQL pattern) */
	if (keys && scan->numberOfKeys > 0)
		memcpy(scan->keyData, keys, scan->numberOfKeys * sizeof(ScanKeyData));

	/* Cache the search key datum (int4 equality for checkpoint 1) */
	if (scan->numberOfKeys > 0)
		so->search_key = DatumGetInt32(scan->keyData[0].sk_argument);
}

/* -----------------------------------------------------------------------
 * fiting_gettuple
 *
 * Return the next matching tuple.  For checkpoint 1 (equality only), this
 * returns at most one result and sets so->done = true afterward.
 * ----------------------------------------------------------------------- */
bool
fiting_gettuple(IndexScanDesc scan, ScanDirection direction)
{
	FitingScanOpaque	so = (FitingScanOpaque) scan->opaque;
	Relation			index = scan->indexRelation;
	int32				search_key = so->search_key;

	/* Meta page fields */
	int64			total_tuples;
	int32			max_error;
	int32			num_segments;

	/* Working variables */
	Buffer				buf;
	Page				page;
	FitingMetaPageData *meta;
	FitingDirEntry	   *dir;
	FitingDirEntry	   *dir_copy;
	int					seg_idx;
	const FitingDirEntry *seg;
	int64				predicted;
	int64				lo, hi;
	FitingLeafTuple		tup;

	/* Equality scan returns at most one result */
	if (so->done)
		return false;
	so->done = true;

	/* ---- Read meta page ---------------------------------------------- */
	buf = ReadBuffer(index, FITING_META_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	meta = FitingPageGetMeta(page);

	if (meta->magic != FITING_MAGIC)
	{
		UnlockReleaseBuffer(buf);
		elog(ERROR, "fiting_tree: bad magic number in meta page");
	}

	total_tuples = meta->total_tuples;
	max_error    = meta->max_error;
	num_segments = meta->num_segments;
	UnlockReleaseBuffer(buf);

	if (total_tuples == 0 || num_segments == 0)
		return false;

	/* ---- Read and copy the directory --------------------------------- */
	buf = ReadBuffer(index, FITING_DIR_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	dir = FitingPageGetDir(page);

	dir_copy = palloc(num_segments * sizeof(FitingDirEntry));
	memcpy(dir_copy, dir, num_segments * sizeof(FitingDirEntry));
	UnlockReleaseBuffer(buf);

	/* ---- Find the segment that covers search_key --------------------- */
	seg_idx = find_segment_index(dir_copy, num_segments, search_key);
	if (seg_idx < 0)
	{
		/* search_key is below the first segment's start_key */
		pfree(dir_copy);
		return false;
	}
	seg = &dir_copy[seg_idx];

	/* ---- Predict rank and compute search window ---------------------- */
	if (search_key == seg->start_key)
		predicted = seg->base_rank;
	else
		predicted = seg->base_rank +
			(int64) ((double) (search_key - seg->start_key) * seg->slope);

	lo = predicted - max_error;
	hi = predicted + max_error;

	if (lo < 0) lo = 0;
	if (hi >= total_tuples) hi = total_tuples - 1;

	pfree(dir_copy);

	/* ---- Binary search in leaf pages within [lo, hi] ----------------- */
	while (lo <= hi)
	{
		int64 mid = lo + (hi - lo) / 2;

		if (!get_leaf_tuple_at_rank(index, mid, total_tuples, &tup))
			break;

		if (tup.key == search_key)
		{
			/* Found — return this heap TID */
			scan->xs_heaptid = tup.tid;
			scan->xs_recheck = false;
			return true;
		}
		else if (tup.key < search_key)
		{
			lo = mid + 1;
		}
		else
		{
			hi = mid - 1;
		}
	}

	return false;
}

/* -----------------------------------------------------------------------
 * fiting_endscan
 * ----------------------------------------------------------------------- */
void
fiting_endscan(IndexScanDesc scan)
{
	FitingScanOpaque so = (FitingScanOpaque) scan->opaque;

	pfree(so);
	scan->opaque = NULL;
}
