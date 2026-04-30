/*-------------------------------------------------------------------------
 *
 * ft_scan.c
 *   FITing-Tree index scan (ambeginscan / amrescan / amgettuple / amendscan).
 *
 * Lookup algorithm (non-contiguous segment pages via linked list):
 *
 *   Phase 1 — data pages:
 *     1. Read meta → max_error.
 *     2. Read and cache full directory page (FitingDirPageContent).
 *     3. Binary-search directory entries for the segment whose
 *        start_key ≤ search_key.
 *     4. Predict local rank:
 *          local_pred = (search_key − seg.start_key) × seg.slope
 *     5. Clamp to [0, seg.seg_total_tuples−1] ± max_error.
 *     6. Binary-search within the window by walking the segment's page
 *        linked list.  Skip FITING_LEAF_DELETED entries; on a deleted
 *        hit, scan linearly left/right within [lo, hi].
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
 * Binary-search directory entries for the largest i where
 * entries[i].start_key ≤ key.  Returns -1 if key < entries[0].start_key.
 * ----------------------------------------------------------------------- */
static int
find_segment_index(const FitingDirEntry *entries, int num_segs, int64 key)
{
	int lo = 0, hi = num_segs - 1, result = -1;

	while (lo <= hi)
	{
		int mid = (lo + hi) / 2;

		if (entries[mid].start_key <= key)
		{
			result = mid;
			lo     = mid + 1;
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
 * Read the FitingLeafTuple at local position local_rank within segment
 * seg by walking the segment's page linked list stored in pool[].
 *
 * Address formula (same semantics as before; now traverses the list):
 *   absolute = seg->start_slot + local_rank
 *   page_idx = absolute / FITING_TUPLES_PER_PAGE   (how far to walk)
 *   slot     = absolute % FITING_TUPLES_PER_PAGE   (slot within that page)
 *
 * Returns false if the position is out of range.
 * ----------------------------------------------------------------------- */
static bool
get_seg_tuple_at_local_rank(Relation index,
							 const FitingDirEntry *seg,
							 const FitingDirPageContent *dir,
							 int64 local_rank,
							 FitingLeafTuple *out)
{
	int64		absolute;
	int64		page_idx;
	int			slot;
	int			node_idx;
	int64		p;
	Buffer		buf;
	Page		page;
	FitingLeafTuple *tuples;
	FitingPageListNode nd;

	if (local_rank < 0 || local_rank >= seg->seg_total_tuples)
		return false;

	absolute = (int64) seg->start_slot + local_rank;
	page_idx = absolute / FITING_TUPLES_PER_PAGE;
	slot     = (int) (absolute % FITING_TUPLES_PER_PAGE);

	/* Walk the linked list to the page_idx-th node */
	node_idx = seg->page_list_head;
	for (p = 0; p < page_idx; p++)
	{
		if (node_idx < 0)
			return false;
		fiting_get_node(index, dir, node_idx, &nd);
		node_idx = nd.next;
	}
	if (node_idx < 0)
		return false;

	fiting_get_node(index, dir, node_idx, &nd);
	buf    = ReadBuffer(index, nd.page_no);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page   = BufferGetPage(buf);
	tuples = FitingPageGetLeaf(page);
	*out   = tuples[slot];
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
search_seg_data(Relation index,
				const FitingDirEntry *seg,
				const FitingDirPageContent *dir,
				int64 lo, int64 hi,
				int64 search_key,
				ItemPointerData *result_tid)
{
	FitingLeafTuple tup;

	while (lo <= hi)
	{
		int64 mid = lo + (hi - lo) / 2;

		if (!get_seg_tuple_at_local_rank(index, seg, dir, mid, &tup))
			break;

		if (tup.key == search_key)
		{
			if (!(tup.flags & FITING_LEAF_DELETED))
			{
				*result_tid = tup.tid;
				return true;
			}

			/* Tombstone — scan left then right for a live copy */
			for (int64 r = mid - 1; r >= lo; r--)
			{
				if (!get_seg_tuple_at_local_rank(index, seg, dir, r, &tup))
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
				if (!get_seg_tuple_at_local_rank(index, seg, dir, r, &tup))
					break;
				if (tup.key != search_key)
					break;
				if (!(tup.flags & FITING_LEAF_DELETED))
				{
					*result_tid = tup.tid;
					return true;
				}
			}
			return false;	/* key found but all copies deleted */
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
search_seg_buffer(Relation index,
				  const FitingDirEntry *seg,
				  int64 search_key,
				  ItemPointerData *result_tid)
{
	Buffer			buf_rel;
	Page			page;
	FitingLeafTuple *tuples;
	int				num_buf;
	int				lo, hi;

	num_buf = FitingBufCount(seg);	/* declared above, set here */
	if (seg->buf_blkno == InvalidBlockNumber || num_buf == 0)
		return false;

	buf_rel = ReadBuffer(index, seg->buf_blkno);
	LockBuffer(buf_rel, BUFFER_LOCK_SHARE);
	page    = BufferGetPage(buf_rel);
	/* Offset into the shared page to reach this segment's 32-tuple slot */
	tuples  = FitingPageGetLeaf(page) +
			  FitingBufSlot(seg) * FITING_BUF_TUPLES_PER_SEG;

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
 * search_btree_seg
 *
 * Locate search_key within a BTREE segment using the per-page start_key
 * stored in pool nodes.
 *
 * Cost:
 *   - O(pages_in_seg) in-memory pool walk  (≤ 8 pool-array accesses, no I/O;
 *     pool is the palloc'd in-memory copy held in the scan opaque)
 *   - Exactly 1 ReadBuffer on the identified target page
 *   - O(log FITING_TUPLES_PER_PAGE) binary search within that page
 *
 * Returns true and fills *result_tid if a live tuple with search_key is found.
 * ----------------------------------------------------------------------- */
static bool
search_btree_seg(Relation index,
				 const FitingDirEntry      *seg,
				 const FitingDirPageContent *dir,
				 int64					    search_key,
				 ItemPointerData		   *result_tid)
{
	int				node_idx = seg->page_list_head;
	int				target   = -1;
	Buffer			buf;
	Page			page;
	FitingLeafTuple *tuples;
	int				ntuples;
	int				lo, hi;
	FitingPageListNode nd;

	/* Key is before the first key stored in this segment */
	if (node_idx < 0)
		return false;
	fiting_get_node(index, dir, node_idx, &nd);
	if (nd.page_start_key > search_key)
		return false;

	/*
	 * Walk pool nodes to find the page whose range
	 * [page_start_key, next_page_start_key) contains search_key.
	 * For BTREE segments there are at most FITING_BTREE_WINDOW / 510 ≈ 8
	 * pages; primary-pool nodes require no I/O.
	 */
	while (node_idx >= 0)
	{
		FitingPageListNode cur_nd;
		int next_idx;

		fiting_get_node(index, dir, node_idx, &cur_nd);
		next_idx = cur_nd.next;

		if (next_idx < 0)
		{
			target = node_idx;
			break;
		}

		{
			FitingPageListNode next_nd;

			fiting_get_node(index, dir, next_idx, &next_nd);
			if (next_nd.page_start_key > search_key)
			{
				target = node_idx;
				break;
			}
		}
		node_idx = next_idx;
	}

	if (target < 0)
		return false;

	/* ── Single ReadBuffer ─────────────────────────────────────────────── */
	fiting_get_node(index, dir, target, &nd);
	buf    = ReadBuffer(index, nd.page_no);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page   = BufferGetPage(buf);
	tuples = FitingPageGetLeaf(page);

	ntuples = FitingPageGetNumTuples(page);

	lo = 0;
	hi = ntuples - 1;
	while (lo <= hi)
	{
		int mid = lo + (hi - lo) / 2;

		if (tuples[mid].key == search_key)
		{
			if (!(tuples[mid].flags & FITING_LEAF_DELETED))
			{
				*result_tid = tuples[mid].tid;
				UnlockReleaseBuffer(buf);
				return true;
			}
			/* Tombstone — scan left then right for a live copy */
			{
				int r;

				for (r = mid - 1; r >= lo; r--)
				{
					if (tuples[r].key != search_key) break;
					if (!(tuples[r].flags & FITING_LEAF_DELETED))
					{
						*result_tid = tuples[r].tid;
						UnlockReleaseBuffer(buf);
						return true;
					}
				}
				for (r = mid + 1; r <= hi; r++)
				{
					if (tuples[r].key != search_key) break;
					if (!(tuples[r].flags & FITING_LEAF_DELETED))
					{
						*result_tid = tuples[r].tid;
						UnlockReleaseBuffer(buf);
						return true;
					}
				}
			}
			UnlockReleaseBuffer(buf);
			return false;	/* key present but all copies deleted */
		}
		else if (tuples[mid].key < search_key)
			lo = mid + 1;
		else
			hi = mid - 1;
	}

	UnlockReleaseBuffer(buf);
	return false;
}

/* -----------------------------------------------------------------------
 * fiting_beginscan
 * ----------------------------------------------------------------------- */
IndexScanDesc
fiting_beginscan(Relation indexRelation, int nkeys, int norderbys)
{
	IndexScanDesc	 scan;
	FitingScanOpaque so;

	scan = RelationGetIndexScan(indexRelation, nkeys, norderbys);

	so = palloc0(sizeof(FitingScanOpaqueData));
	so->data_done  = false;
	so->buf_done   = false;
	so->dir_copy   = NULL;
	so->seg_idx    = -1;
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

	so->data_done = false;
	so->buf_done  = false;
	so->seg_idx   = -1;

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
	FitingScanOpaque	so        = (FitingScanOpaque) scan->opaque;
	Relation			index     = scan->indexRelation;
	int64				search_key = so->search_key;

	/* ---- Phase 1: data pages ----------------------------------------- */
	if (!so->data_done)
	{
		FitingDirEntry		*seg;
		ItemPointerData		 result_tid;

		so->data_done = true;

		/* Load and cache full directory page on first call */
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

			so->max_error = meta->max_error;
			so->num_segs  = meta->num_segments;
			UnlockReleaseBuffer(buf);

			/*
			 * Check for empty index BEFORE reading the directory page.
			 * On a table built empty, block 1 still exists but has no entries;
			 * reading it is harmless, but short-circuiting here avoids the
			 * unnecessary I/O and guards against any edge-case where block 1
			 * was not written (older on-disk format).
			 */
			if (so->num_segs == 0)
			{
				so->buf_done = true;
				return false;
			}

			so->dir_copy = fiting_read_dir_copy(index);
		}

		so->seg_idx = find_segment_index(so->dir_copy->entries, so->num_segs,
										 search_key);
		if (so->seg_idx < 0)
			goto phase2;

		seg = &so->dir_copy->entries[so->seg_idx];

		if (FitingSegType(seg) == FITING_SEG_TYPE_FITING)
		{
			/*
			 * FITING path: use the linear model to predict the rank, then
			 * binary-search within the narrow [lo, hi] window.
			 */
			int64 local_pred = (int64) ((double) (search_key - seg->start_key)
										* seg->slope);
			int64 lo         = local_pred - so->max_error;
			int64 hi         = local_pred + so->max_error;

			if (lo < 0)
				lo = 0;
			if (hi >= seg->seg_total_tuples)
				hi = seg->seg_total_tuples - 1;

			if (search_seg_data(index, seg, so->dir_copy, lo, hi,
								search_key, &result_tid))
			{
				scan->xs_heaptid = result_tid;
				scan->xs_recheck = false;
				return true;
			}
		}
		else
		{
			/*
			 * BTREE path: pool walk to identify the target page by
			 * page_start_key, then exactly 1 ReadBuffer + in-page binary search.
			 * Primary nodes need no disk I/O; overflow nodes use ReadBuffer.
			 */
			if (search_btree_seg(index, seg, so->dir_copy, search_key,
								 &result_tid))
			{
				scan->xs_heaptid = result_tid;
				scan->xs_recheck = false;
				return true;
			}
		}
	}

phase2:
	/* ---- Phase 2: buffer page ---------------------------------------- */
	if (!so->buf_done)
	{
		ItemPointerData result_tid;

		so->buf_done = true;

		if (so->seg_idx >= 0 && so->dir_copy != NULL)
		{
			FitingDirEntry *seg = &so->dir_copy->entries[so->seg_idx];

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
