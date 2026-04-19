/*-------------------------------------------------------------------------
 *
 * ft_scan.c
 *   FITing-Tree index scan (ambeginscan / amrescan / amgettuple / amendscan).
 *
 * Lookup algorithm (Checkpoint 2 — per-segment page ownership):
 *
 *   Phase 1 — data pages:
 *     1. Read meta page → max_error, num_segments.
 *     2. Read and cache directory page.
 *     3. Binary-search directory for the segment whose start_key ≤ search_key.
 *     4. Predict local rank:
 *          local_pred = (search_key − seg.start_key) × seg.slope
 *     5. Clamp to [0, seg.seg_total_tuples−1] ± max_error.
 *     6. Binary-search the segment's data pages within the window.
 *        Skip entries flagged FITING_LEAF_DELETED.
 *        On a deleted hit for the right key, scan linearly left/right.
 *     7. If found: set scan->xs_heaptid, return true.
 *
 *   Phase 2 — buffer page (if Phase 1 missed):
 *     8. Binary-search the segment's buffer page (always live).
 *     9. If found: set scan->xs_heaptid, return true.
 *
 *   10. Return false.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relscan.h"
#include "fiting_tree.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/* -----------------------------------------------------------------------
 * find_segment_index
 *
 * Binary-search the directory for the largest i where dir[i].start_key ≤ key.
 * Returns -1 if key < dir[0].start_key.
 * ----------------------------------------------------------------------- */
static int
find_segment_index(const FitingDirEntry *dir, int num_segs, int64 key)
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
 * get_seg_tuple_at_local_rank
 *
 * Read the FitingLeafTuple at local position `local_rank` within segment
 * `seg`.  Returns false if the position is out of range.
 *
 * Rank-to-address formula:
 *   absolute = seg->start_slot + local_rank
 *   page_idx = absolute / FITING_TUPLES_PER_PAGE
 *   slot     = absolute % FITING_TUPLES_PER_PAGE
 * ----------------------------------------------------------------------- */
static bool
get_seg_tuple_at_local_rank(Relation index, const FitingDirEntry *seg,
							 int64 local_rank, FitingLeafTuple *out)
{
	int			absolute;
	int			page_idx;
	int			slot;
	Buffer		buf;
	Page		page;
	FitingLeafTuple *tuples;

	if (local_rank < 0 || local_rank >= seg->seg_total_tuples)
		return false;

	absolute = (int) seg->start_slot + (int) local_rank;
	page_idx = absolute / FITING_TUPLES_PER_PAGE;
	slot     = absolute % FITING_TUPLES_PER_PAGE;

	if (page_idx >= seg->num_data_pages)
		return false;

	buf = ReadBuffer(index, seg->first_data_blkno + page_idx);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	tuples = FitingPageGetLeaf(page);
	*out = tuples[slot];
	UnlockReleaseBuffer(buf);

	return true;
}

/* -----------------------------------------------------------------------
 * search_seg_data
 *
 * Binary-search segment seg's data pages for search_key within local rank
 * window [lo, hi].  Skips FITING_LEAF_DELETED entries: on a deleted key
 * match, scans linearly left then right within the window for a live copy.
 *
 * Returns true and fills *result_tid if found.
 * ----------------------------------------------------------------------- */
static bool
search_seg_data(Relation index, const FitingDirEntry *seg,
				int64 lo, int64 hi, int64 search_key,
				ItemPointerData *result_tid)
{
	FitingLeafTuple tup;

	while (lo <= hi)
	{
		int64 mid = lo + (hi - lo) / 2;

		if (!get_seg_tuple_at_local_rank(index, seg, mid, &tup))
			break;

		if (tup.key == search_key)
		{
			if (!(tup.flags & FITING_LEAF_DELETED))
			{
				*result_tid = tup.tid;
				return true;
			}

			/*
			 * Key matches but entry is a tombstone.  Scan linearly left
			 * then right within [lo, hi] for a live copy of the same key.
			 */
			for (int64 r = mid - 1; r >= lo; r--)
			{
				if (!get_seg_tuple_at_local_rank(index, seg, r, &tup))
					break;
				if (tup.key != search_key)
					break;
				if (!(tup.flags & FITING_LEAF_DELETED))
				{
					*result_tid = tup.tid;
					return true;
				}
			}
			for (int64 r = mid + 1; r <= hi; r++)
			{
				if (!get_seg_tuple_at_local_rank(index, seg, r, &tup))
					break;
				if (tup.key != search_key)
					break;
				if (!(tup.flags & FITING_LEAF_DELETED))
				{
					*result_tid = tup.tid;
					return true;
				}
			}
			/* Key found but all copies deleted */
			return false;
		}
		else if (tup.key < search_key)
			lo = mid + 1;
		else
			hi = mid - 1;
	}

	return false;
}

/* -----------------------------------------------------------------------
 * search_seg_buffer
 *
 * Binary-search the segment's buffer page for search_key.
 * Buffer entries are always live (no FITING_LEAF_DELETED in buffer).
 * Returns true and fills *result_tid if found.
 * ----------------------------------------------------------------------- */
static bool
search_seg_buffer(Relation index, const FitingDirEntry *seg,
				  int64 search_key, ItemPointerData *result_tid)
{
	Buffer			buf_rel;
	Page			page;
	FitingLeafTuple *tuples;
	int				num_buf;
	int				lo, hi;

	if (seg->buf_blkno == InvalidBlockNumber || seg->num_buffer_tuples == 0)
		return false;

	buf_rel = ReadBuffer(index, seg->buf_blkno);
	LockBuffer(buf_rel, BUFFER_LOCK_SHARE);
	page   = BufferGetPage(buf_rel);
	tuples = FitingPageGetLeaf(page);
	num_buf = seg->num_buffer_tuples;

	lo = 0;
	hi = num_buf - 1;

	while (lo <= hi)
	{
		int mid = lo + (hi - lo) / 2;

		if (tuples[mid].key == search_key)
		{
			*result_tid = tuples[mid].tid;
			UnlockReleaseBuffer(buf_rel);
			return true;
		}
		else if (tuples[mid].key < search_key)
			lo = mid + 1;
		else
			hi = mid - 1;
	}

	UnlockReleaseBuffer(buf_rel);
	return false;
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
	so->data_done = false;
	so->buf_done  = false;
	so->dir_copy  = NULL;
	so->seg_idx   = -1;
	so->search_key = 0;

	scan->opaque = so;
	return scan;
}

/* -----------------------------------------------------------------------
 * fiting_rescan
 * ----------------------------------------------------------------------- */
void
fiting_rescan(IndexScanDesc scan, ScanKey keys, int nkeys,
			  ScanKey orderbys, int norderbys)
{
	FitingScanOpaque so = (FitingScanOpaque) scan->opaque;

	/* Reset scan state for a new search */
	so->data_done = false;
	so->buf_done  = false;
	so->seg_idx   = -1;

	/* Release cached directory if present */
	if (so->dir_copy)
	{
		pfree(so->dir_copy);
		so->dir_copy = NULL;
	}

	if (keys && scan->numberOfKeys > 0)
		memcpy(scan->keyData, keys, scan->numberOfKeys * sizeof(ScanKeyData));

	if (scan->numberOfKeys > 0)
		so->search_key = FitingDatumGetKey(
			scan->keyData[0].sk_argument,
			TupleDescAttr(scan->indexRelation->rd_att, 0)->atttypid);
}

/* -----------------------------------------------------------------------
 * fiting_gettuple
 *
 * Two-phase lookup: data pages first, then buffer page.
 * ----------------------------------------------------------------------- */
bool
fiting_gettuple(IndexScanDesc scan, ScanDirection direction)
{
	FitingScanOpaque	so    = (FitingScanOpaque) scan->opaque;
	Relation			index = scan->indexRelation;
	int64				search_key = so->search_key;

	/* ---- Phase 1: data pages ----------------------------------------- */
	if (!so->data_done)
	{
		FitingDirEntry	   *seg;
		int64				local_pred;
		int64				lo, hi;
		ItemPointerData		result_tid;

		so->data_done = true;

		/* Load and cache directory + meta on first call */
		if (so->dir_copy == NULL)
		{
			Buffer				buf;
			Page				page;
			FitingMetaPageData *meta;

			buf  = ReadBuffer(index, FITING_META_BLKNO);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);
			meta = FitingPageGetMeta(page);

			if (meta->magic != FITING_MAGIC)
			{
				UnlockReleaseBuffer(buf);
				elog(ERROR, "fiting_tree: bad magic in meta page");
			}

			so->num_segs   = meta->num_segments;
			so->max_error  = meta->max_error;
			UnlockReleaseBuffer(buf);

			if (so->num_segs == 0)
			{
				so->buf_done = true;	/* nothing to search */
				return false;
			}

			buf  = ReadBuffer(index, FITING_DIR_BLKNO);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);

			so->dir_copy = palloc(so->num_segs * sizeof(FitingDirEntry));
			memcpy(so->dir_copy, FitingPageGetDir(page),
				   so->num_segs * sizeof(FitingDirEntry));
			UnlockReleaseBuffer(buf);
		}

		so->seg_idx = find_segment_index(so->dir_copy, so->num_segs,
										 search_key);
		if (so->seg_idx < 0)
			goto phase2;	/* key before all segment start_keys */

		seg = &so->dir_copy[so->seg_idx];

		/* Predict local rank within this segment */
		local_pred = (int64) ((double) (search_key - seg->start_key)
							  * seg->slope);

		lo = local_pred - so->max_error;
		hi = local_pred + so->max_error;

		if (lo < 0)
			lo = 0;
		if (hi >= seg->seg_total_tuples)
			hi = seg->seg_total_tuples - 1;

		if (search_seg_data(index, seg, lo, hi, search_key, &result_tid))
		{
			scan->xs_heaptid = result_tid;
			scan->xs_recheck = false;
			return true;
		}
	}

phase2:
	/* ---- Phase 2: buffer page ---------------------------------------- */
	if (!so->buf_done)
	{
		ItemPointerData result_tid;

		so->buf_done = true;

		/*
		 * If seg_idx < 0 (key is before all segments) we have no segment
		 * to check a buffer for — skip.
		 */
		if (so->seg_idx >= 0 && so->dir_copy != NULL)
		{
			FitingDirEntry *seg = &so->dir_copy[so->seg_idx];

			if (search_seg_buffer(index, seg, search_key, &result_tid))
			{
				scan->xs_heaptid = result_tid;
				scan->xs_recheck = false;
				return true;
			}
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

	if (so->dir_copy)
		pfree(so->dir_copy);

	pfree(so);
	scan->opaque = NULL;
}
