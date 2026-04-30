/*-------------------------------------------------------------------------
 *
 * ft_build.c
 *   FITing-Tree index build (ambuild / ambuildempty) and shared page
 *   management helpers used by ft_insert.c and ft_vacuum.c.
 *
 * Key differences from the original contiguous design:
 *
 *   1. Segment pages are described by a linked list of FitingPageListNode
 *      records stored in a pool on the directory page.  Pages no longer
 *      need to be physically contiguous.
 *
 *   2. The meta page carries page_tuple_counts[] — a per-block ref-count.
 *      fiting_free_page() only wipes a block when its count drops to 0,
 *      so a page shared between two segments at bulk-build time survives
 *      when one segment is resegmented.
 *
 *   3. fiting_resegment() reuses old segment pages in-place (full overwrite,
 *      counts set to 1), then appends to the global data_curr_page if it has
 *      capacity, and finally falls back to the freelist / ExtendBufferedRel.
 *      Surplus old pages are freed after writing.
 *
 *   4. Buffer pages are shared: up to FITING_BUF_SLOTS_PER_PAGE (15) segments
 *      share one 8 kB page.  Each segment owns a fixed 32-tuple slot tracked
 *      by FitingDirEntry.buf_info (slot index + count, packed).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>				/* qsort */

#include "access/genam.h"
#include "access/generic_xlog.h"
#include "access/tableam.h"
#include "fiting_tree.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* -----------------------------------------------------------------------
 * Build state accumulated during the heap scan
 * ----------------------------------------------------------------------- */
typedef struct FitingBuildState
{
	int64			ntups;
	int64			alloc_size;
	FitingLeafTuple *tuples;
	MemoryContext	tmpCtx;
	int32			max_error;
	Oid				keytype;
} FitingBuildState;

/* -----------------------------------------------------------------------
 * FinalEntry — one directory entry emitted by classify_segments().
 *
 * A FITING entry maps 1-to-1 from a ShrinkingCone segment (from_seg==to_seg).
 * A BTREE  entry merges segs[from_seg .. to_seg] into one directory entry,
 * looked up via the in-memory page_start_key walk + 1 ReadBuffer.
 * ----------------------------------------------------------------------- */
typedef struct FinalEntry
{
	int		from_seg;		/* first constituent ShrinkingCone seg index */
	int		to_seg;			/* last  constituent ShrinkingCone seg index */
	int		seg_type;		/* FITING_SEG_TYPE_FITING or _BTREE */
	int64	total_tuples;	/* sum of constituent segment sizes */
} FinalEntry;

/* -----------------------------------------------------------------------
 * classify_segments
 *
 * Post-ShrinkingCone grouping pass.  Scans the raw segment array in windows
 * of FITING_BTREE_WINDOW tuples.  A window whose segment count exceeds
 * (tuples_in_window / 100) is emitted as a single BTREE directory entry;
 * otherwise each segment in the window becomes an individual FITING entry.
 *
 * seg_sizes[i] = number of tuples in segs[i], pre-computed by the caller
 *   from consecutive base_rank differences.
 *
 * Returns a palloc'd FinalEntry array; *nfinals_out receives the count.
 * ----------------------------------------------------------------------- */
/*
 * classify_segments
 *
 * window: flush window size in tuples (default FITING_BTREE_WINDOW).
 * thresh: max segments-per-window before BTREE grouping kicks in
 *         (default FITING_BTREE_SEG_THRESH).
 *
 * The BTREE condition scales proportionally with actual window occupancy:
 *   win_nsegs * window > win_tuples * thresh
 * This is equivalent to win_nsegs > thresh * (win_tuples / window), so a
 * partially-filled window at the end of the array gets a proportionally
 * reduced threshold.
 */
static FinalEntry *
classify_segments(const FitingSegment *segs, const int64 *seg_sizes,
				  int num_segs, int64 window, int thresh, int *nfinals_out)
{
	/* Worst case: every segment is its own FITING entry */
	FinalEntry *finals  = palloc(num_segs * sizeof(FinalEntry));
	int			nfinals = 0;

	int		win_start  = 0;		/* first seg index in current window */
	int64	win_tuples = 0;		/* accumulated tuples */
	int		win_nsegs  = 0;		/* accumulated segment count */
	int		i;

	for (i = 0; i <= num_segs; i++)
	{
		bool end_of_input = (i == num_segs);
		bool flush;

		flush = end_of_input ||
				(win_nsegs > 0 &&
				 win_tuples + seg_sizes[i] > window);

		if (!flush)
		{
			win_tuples += seg_sizes[i];
			win_nsegs++;
			continue;
		}

		/* Flush window [win_start .. i-1] */
		if (win_nsegs > 0)
		{
			/*
			 * Proportional threshold: segments-per-window exceeds thresh.
			 * Use cross-multiply to avoid division and stay in integers.
			 *   use_btree iff  win_nsegs / win_tuples > thresh / window
			 *              iff  win_nsegs * window     > win_tuples * thresh
			 */
			bool use_btree = ((int64) win_nsegs * window >
							  win_tuples * (int64) thresh);

			if (use_btree)
			{
				finals[nfinals].from_seg     = win_start;
				finals[nfinals].to_seg       = i - 1;
				finals[nfinals].seg_type     = FITING_SEG_TYPE_BTREE;
				finals[nfinals].total_tuples = win_tuples;
				nfinals++;
			}
			else
			{
				int j;

				for (j = win_start; j < i; j++)
				{
					finals[nfinals].from_seg     = j;
					finals[nfinals].to_seg       = j;
					finals[nfinals].seg_type     = FITING_SEG_TYPE_FITING;
					finals[nfinals].total_tuples = seg_sizes[j];
					nfinals++;
				}
			}
		}

		/* Start fresh window at segment i */
		win_start  = i;
		win_tuples = end_of_input ? 0 : seg_sizes[i];
		win_nsegs  = end_of_input ? 0 : 1;
	}

	*nfinals_out = nfinals;
	return finals;
}

/* -----------------------------------------------------------------------
 * Comparison function for qsort
 * ----------------------------------------------------------------------- */
static int
leaf_tuple_cmp(const void *a, const void *b)
{
	const FitingLeafTuple *ta = (const FitingLeafTuple *) a;
	const FitingLeafTuple *tb = (const FitingLeafTuple *) b;

	if (ta->key < tb->key) return -1;
	if (ta->key > tb->key) return  1;
	return 0;
}

/* -----------------------------------------------------------------------
 * Per-tuple callback called by table_index_build_scan
 * ----------------------------------------------------------------------- */
static void
fiting_build_callback(Relation index, ItemPointer tid, Datum *values,
					  bool *isnull, bool tupleIsAlive, void *state)
{
	FitingBuildState *bs = (FitingBuildState *) state;
	MemoryContext	oldCtx;

	if (!tupleIsAlive || isnull[0])
		return;

	oldCtx = MemoryContextSwitchTo(bs->tmpCtx);

	if (bs->ntups >= bs->alloc_size)
	{
		bs->alloc_size *= 2;
		bs->tuples = repalloc(bs->tuples,
							  bs->alloc_size * sizeof(FitingLeafTuple));
	}

	bs->tuples[bs->ntups].key   = FitingDatumGetKey(values[0], bs->keytype);
	bs->tuples[bs->ntups].tid   = *tid;
	bs->tuples[bs->ntups].flags = 0;
	bs->ntups++;

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(bs->tmpCtx);
}

/* -----------------------------------------------------------------------
 * write_page_to_index
 *
 * Append a new page via ExtendBufferedRel, fill with data, flush via WAL.
 * Used only during the initial bulk build for data pages.
 * ----------------------------------------------------------------------- */
static void
write_page_to_index(Relation index, uint16 page_flags,
					const void *data, size_t data_bytes)
{
	Buffer				buf;
	Page				page;
	GenericXLogState   *xstate;

	buf = ExtendBufferedRel(BMR_REL(index), MAIN_FORKNUM, NULL, EB_LOCK_FIRST);

	xstate = GenericXLogStart(index);
	page   = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

	FitingInitPage(page, page_flags);
	if (data_bytes > 0)
	{
		memcpy(PageGetContents(page), data, data_bytes);
		((PageHeader) page)->pd_lower += (int) data_bytes;
	}

	GenericXLogFinish(xstate);
	UnlockReleaseBuffer(buf);
}

/* =======================================================================
 * Canonical page I/O helpers (declared in fiting_tree.h; used by all
 * sub-modules via the extern declarations).
 * ======================================================================= */

/* -----------------------------------------------------------------------
 * fiting_read_meta_and_counts
 *
 * Read the meta page: copy the fixed FitingMetaPageData fields into the
 * return value, and copy page_tuple_counts[] into counts_out[].
 * counts_out must point to FITING_META_MAX_PAGES int32 values.
 * ----------------------------------------------------------------------- */
FitingMetaPageData
fiting_read_meta_and_counts(Relation index, int32 *counts_out)
{
	Buffer				buf;
	Page				page;
	FitingMetaPageData *meta_ptr;
	FitingMetaPageData	meta;

	buf      = ReadBuffer(index, FITING_META_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page     = BufferGetPage(buf);
	meta_ptr = FitingPageGetMeta(page);

	if (meta_ptr->magic != FITING_MAGIC)
	{
		UnlockReleaseBuffer(buf);
		elog(ERROR, "fiting_tree: bad magic in meta page");
	}

	meta = *meta_ptr;

	if (counts_out)
		memcpy(counts_out, FitingMetaGetCounts(page),
			   FITING_META_MAX_PAGES * sizeof(int32));

	UnlockReleaseBuffer(buf);
	return meta;
}

/* -----------------------------------------------------------------------
 * fiting_write_meta_and_counts
 *
 * Overwrite the meta page in place with meta + counts[].
 * counts may be NULL when the caller has not loaded/modified counts.
 * ----------------------------------------------------------------------- */
void
fiting_write_meta_and_counts(Relation index, const FitingMetaPageData *meta,
							  const int32 *counts)
{
	Buffer				buf;
	Page				page;
	GenericXLogState   *xstate;
	char			   *dst;
	size_t				total;

	buf = ReadBuffer(index, FITING_META_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	xstate = GenericXLogStart(index);
	page   = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

	FitingInitPage(page, FITING_F_META);
	dst = (char *) PageGetContents(page);

	memcpy(dst, meta, sizeof(FitingMetaPageData));
	total = sizeof(FitingMetaPageData);

	if (counts)
	{
		memcpy(dst + sizeof(FitingMetaPageData), counts,
			   FITING_META_MAX_PAGES * sizeof(int32));
		total += FITING_META_MAX_PAGES * sizeof(int32);
	}

	((PageHeader) page)->pd_lower += (int) total;

	GenericXLogFinish(xstate);
	UnlockReleaseBuffer(buf);
}

/* -----------------------------------------------------------------------
 * fiting_read_dir_copy
 *
 * Read the directory page and return a palloc'd copy of the full
 * FitingDirPageContent (header + entries + node pool).
 * ----------------------------------------------------------------------- */
FitingDirPageContent *
fiting_read_dir_copy(Relation index)
{
	Buffer				buf;
	Page				page;
	FitingDirPageContent *copy;

	buf  = ReadBuffer(index, FITING_DIR_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	copy = palloc(sizeof(FitingDirPageContent));
	memcpy(copy, FitingPageGetDirContent(page), sizeof(FitingDirPageContent));

	UnlockReleaseBuffer(buf);
	return copy;
}

/* -----------------------------------------------------------------------
 * fiting_write_dir_page
 *
 * Overwrite the directory page with the in-memory content.
 * ----------------------------------------------------------------------- */
void
fiting_write_dir_page(Relation index, const FitingDirPageContent *dir)
{
	Buffer				buf;
	Page				page;
	GenericXLogState   *xstate;

	buf  = ReadBuffer(index, FITING_DIR_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	xstate = GenericXLogStart(index);
	page   = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

	FitingInitPage(page, FITING_F_DIR);
	memcpy(PageGetContents(page), dir, sizeof(FitingDirPageContent));
	((PageHeader) page)->pd_lower += sizeof(FitingDirPageContent);

	GenericXLogFinish(xstate);
	UnlockReleaseBuffer(buf);
}

/* =======================================================================
 * Overflow-capable directory node helpers
 * ======================================================================= */

/* -----------------------------------------------------------------------
 * fiting_get_node
 *
 * Read the FitingPageListNode at global index node_idx into *out.
 * Nodes 0..FITING_DIR_MAX_NODES-1 are served from the in-memory dir copy.
 * Higher indices walk the overflow page chain via ReadBuffer.
 * ----------------------------------------------------------------------- */
void
fiting_get_node(Relation index, const FitingDirPageContent *dir,
				int node_idx, FitingPageListNode *out)
{
	int			ov_idx;
	int			page_num;
	int			slot;
	int			i;
	BlockNumber	blkno;
	Buffer		buf;
	Page		page;

	if (node_idx < 0)
		elog(ERROR, "fiting_tree: fiting_get_node called with node_idx=%d",
			 node_idx);

	if (node_idx < FITING_DIR_MAX_NODES)
	{
		*out = dir->pool[node_idx];
		return;
	}

	ov_idx   = node_idx - FITING_DIR_MAX_NODES;
	page_num = ov_idx / FITING_DIR_OVERFLOW_NODES;
	slot     = ov_idx % FITING_DIR_OVERFLOW_NODES;

	blkno = dir->hdr.next_page;

	/* Walk page_num hops along the overflow chain */
	for (i = 0; i < page_num; i++)
	{
		FitingDirOverflowHeader *ohdr;

		if (blkno == InvalidBlockNumber)
			elog(ERROR,
				 "fiting_tree: overflow dir chain too short at hop %d "
				 "(node_idx=%d)", i, node_idx);

		buf   = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page  = BufferGetPage(buf);
		ohdr  = (FitingDirOverflowHeader *) PageGetContents(page);
		blkno = ohdr->next_page;
		UnlockReleaseBuffer(buf);
	}

	if (blkno == InvalidBlockNumber)
		elog(ERROR, "fiting_tree: overflow dir page not found for node %d",
			 node_idx);

	{
		FitingPageListNode *pool_area;

		buf       = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page      = BufferGetPage(buf);
		pool_area = (FitingPageListNode *)
					((char *) PageGetContents(page) +
					 sizeof(FitingDirOverflowHeader));
		*out      = pool_area[slot];
		UnlockReleaseBuffer(buf);
	}
}

/* -----------------------------------------------------------------------
 * fiting_put_node
 *
 * Write *node to global index node_idx in the directory pool.
 * Primary nodes are updated in the in-memory dir copy (flushed later by
 * fiting_write_dir_page).  Overflow nodes are written immediately via
 * GenericXLog with flag 0 (preserve other slots on the overflow page).
 * ----------------------------------------------------------------------- */
void
fiting_put_node(Relation index, FitingDirPageContent *dir,
				int node_idx, const FitingPageListNode *node)
{
	int			ov_idx;
	int			page_num;
	int			slot;
	int			i;
	BlockNumber	blkno;
	Buffer		buf;
	Page		page;
	GenericXLogState *xstate;
	FitingPageListNode *pool_area;

	if (node_idx < 0)
		elog(ERROR, "fiting_tree: fiting_put_node called with node_idx=%d",
			 node_idx);

	if (node_idx < FITING_DIR_MAX_NODES)
	{
		dir->pool[node_idx] = *node;
		return;
	}

	ov_idx   = node_idx - FITING_DIR_MAX_NODES;
	page_num = ov_idx / FITING_DIR_OVERFLOW_NODES;
	slot     = ov_idx % FITING_DIR_OVERFLOW_NODES;

	blkno = dir->hdr.next_page;

	for (i = 0; i < page_num; i++)
	{
		FitingDirOverflowHeader *ohdr;

		if (blkno == InvalidBlockNumber)
			elog(ERROR,
				 "fiting_tree: overflow dir chain too short (put) at hop %d "
				 "(node_idx=%d)", i, node_idx);

		buf   = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page  = BufferGetPage(buf);
		ohdr  = (FitingDirOverflowHeader *) PageGetContents(page);
		blkno = ohdr->next_page;
		UnlockReleaseBuffer(buf);
	}

	if (blkno == InvalidBlockNumber)
		elog(ERROR,
			 "fiting_tree: overflow dir page not found (put) for node %d",
			 node_idx);

	buf       = ReadBuffer(index, blkno);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	xstate    = GenericXLogStart(index);
	/* flag 0: keep existing content, only update our slot */
	page      = GenericXLogRegisterBuffer(xstate, buf, 0);
	pool_area = (FitingPageListNode *)
				((char *) PageGetContents(page) +
				 sizeof(FitingDirOverflowHeader));
	pool_area[slot] = *node;
	GenericXLogFinish(xstate);
	UnlockReleaseBuffer(buf);
}

/* -----------------------------------------------------------------------
 * alloc_new_overflow_dir_page  [static]
 *
 * Allocate a fresh overflow dir page via fiting_alloc_page, initialise it
 * (FITING_F_DIR flag, zeroed pool), and append it to the overflow chain
 * rooted at dir->hdr.next_page.  Updates dir->hdr.next_page in memory if
 * this is the first overflow page; otherwise updates the old tail via
 * GenericXLog.
 * ----------------------------------------------------------------------- */
static void
alloc_new_overflow_dir_page(Relation index, FitingDirPageContent *dir,
							 FitingMetaPageData *meta, int32 *counts)
{
	BlockNumber		 new_blkno;
	Buffer			 buf;
	Page			 page;
	GenericXLogState *xstate;
	FitingDirOverflowHeader *ohdr;

	new_blkno = fiting_alloc_page(index, meta, counts);

	buf    = ReadBuffer(index, new_blkno);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	xstate = GenericXLogStart(index);
	page   = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

	FitingInitPage(page, FITING_F_DIR);
	ohdr            = (FitingDirOverflowHeader *) PageGetContents(page);
	ohdr->next_page = InvalidBlockNumber;
	ohdr->pad       = 0;
	memset(ohdr + 1, 0,
		   FITING_DIR_OVERFLOW_NODES * sizeof(FitingPageListNode));
	((PageHeader) page)->pd_lower +=
		(int) (sizeof(FitingDirOverflowHeader) +
			   FITING_DIR_OVERFLOW_NODES * sizeof(FitingPageListNode));

	GenericXLogFinish(xstate);
	UnlockReleaseBuffer(buf);

	/* Link the new page into the overflow chain */
	if (dir->hdr.next_page == InvalidBlockNumber)
	{
		/* First overflow page — record in in-memory dir header */
		dir->hdr.next_page = new_blkno;
	}
	else
	{
		/* Find the chain tail and set its next_page */
		BlockNumber cur = dir->hdr.next_page;

		for (;;)
		{
			BlockNumber cur_next;

			buf      = ReadBuffer(index, cur);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page     = BufferGetPage(buf);
			cur_next = ((FitingDirOverflowHeader *) PageGetContents(page))->next_page;
			UnlockReleaseBuffer(buf);

			if (cur_next == InvalidBlockNumber)
			{
				/* cur is the tail — update it */
				buf    = ReadBuffer(index, cur);
				LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
				xstate = GenericXLogStart(index);
				page   = GenericXLogRegisterBuffer(xstate, buf, 0);
				((FitingDirOverflowHeader *) PageGetContents(page))->next_page =
					new_blkno;
				GenericXLogFinish(xstate);
				UnlockReleaseBuffer(buf);
				break;
			}
			cur = cur_next;
		}
	}
}

/* -----------------------------------------------------------------------
 * fiting_dir_alloc_node  [runtime — used by fiting_resegment]
 *
 * Pop a node from the freelist, or claim the next primary slot, or claim
 * the next slot on the current overflow page (allocating a new overflow
 * page when needed).  Returns the global node index.
 * Caller must eventually flush dir via fiting_write_dir_page (for the
 * updated header fields) and meta+counts via fiting_write_meta_and_counts.
 * ----------------------------------------------------------------------- */
int
fiting_dir_alloc_node(FitingDirPageContent *dir, Relation index,
					   FitingMetaPageData *meta, int32 *counts)
{
	int idx;

	/* 1. Pop from freelist (may be a primary or overflow index) */
	if (dir->hdr.pool_freelist >= 0)
	{
		FitingPageListNode fn;

		idx = dir->hdr.pool_freelist;
		fiting_get_node(index, dir, idx, &fn);
		dir->hdr.pool_freelist = fn.next;
		/* Caller will fill the node contents via fiting_put_node */
		return idx;
	}

	/* 2. Claim next primary slot */
	if (dir->hdr.pool_size < FITING_DIR_MAX_NODES)
	{
		idx = dir->hdr.pool_size++;
		/* Primary slot: caller sets via fiting_put_node (or direct dir->pool) */
		return idx;
	}

	/* 3. Need an overflow node */
	{
		int ov_idx = dir->hdr.pool_size - FITING_DIR_MAX_NODES;
		int slot   = ov_idx % FITING_DIR_OVERFLOW_NODES;

		if (slot == 0)
			/* First slot on a new overflow page — allocate the page now */
			alloc_new_overflow_dir_page(index, dir, meta, counts);

		idx = dir->hdr.pool_size++;
		return idx;
	}
}

/* -----------------------------------------------------------------------
 * fiting_dir_free_node  [runtime]
 *
 * Return node_idx to the freelist.  Updates the node's .next to the old
 * freelist head, then sets pool_freelist = node_idx.
 * ----------------------------------------------------------------------- */
void
fiting_dir_free_node(FitingDirPageContent *dir, int node_idx, Relation index)
{
	FitingPageListNode fn;

	Assert(node_idx >= 0 && node_idx < dir->hdr.pool_size);

	fiting_get_node(index, dir, node_idx, &fn);
	fn.page_no        = InvalidBlockNumber;
	fn.page_start_key = 0;
	fn.next           = dir->hdr.pool_freelist;
	fiting_put_node(index, dir, node_idx, &fn);

	dir->hdr.pool_freelist = node_idx;
}

/* -----------------------------------------------------------------------
 * build_alloc_node  [static — build-time only]
 *
 * In-memory-only allocation used during fiting_build Phase 4.  Primary
 * nodes go directly into dir->pool[]; overflow nodes go into the caller's
 * *ovpool array (grown with repalloc as needed).  No disk I/O.
 * ----------------------------------------------------------------------- */
static int
build_alloc_node(FitingDirPageContent *dir,
				 FitingPageListNode **ovpool,
				 int *ov_alloc)
{
	int idx;

	if (dir->hdr.pool_size < FITING_DIR_MAX_NODES)
	{
		idx = dir->hdr.pool_size++;
		dir->pool[idx].page_no        = InvalidBlockNumber;
		dir->pool[idx].next           = -1;
		dir->pool[idx].page_start_key = 0;
		return idx;
	}

	/* Overflow */
	{
		int ov_idx = dir->hdr.pool_size - FITING_DIR_MAX_NODES;

		if (ov_idx >= *ov_alloc)
		{
			*ov_alloc = (*ov_alloc == 0) ? 512 : *ov_alloc * 2;
			if (*ovpool == NULL)
				*ovpool = palloc(*ov_alloc * sizeof(FitingPageListNode));
			else
				*ovpool = repalloc(*ovpool,
								   *ov_alloc * sizeof(FitingPageListNode));
		}

		(*ovpool)[ov_idx].page_no        = InvalidBlockNumber;
		(*ovpool)[ov_idx].next           = -1;
		(*ovpool)[ov_idx].page_start_key = 0;
		idx = dir->hdr.pool_size++;
		return idx;
	}
}

/*
 * BUILD_NODE(ni, dir, ovpool) — pointer to the FitingPageListNode for global
 * index ni, using either the primary dir pool or the build-time overflow array.
 */
#define BUILD_NODE(ni, dir_ptr, ovpool) \
	(((ni) < FITING_DIR_MAX_NODES) \
	 ? &(dir_ptr)->pool[(ni)] \
	 : &(ovpool)[(ni) - FITING_DIR_MAX_NODES])

/* =======================================================================
 * fiting_build
 * ======================================================================= */
IndexBuildResult *
fiting_build(Relation heap, Relation index, struct IndexInfo *indexInfo)
{
	FitingBuildState	 bs;
	IndexBuildResult	*result;
	double				 reltuples;
	int64				*keys;
	FitingSegment		*segs;
	int					 num_segs;
	FitingMetaPageData	 meta;
	int32				*counts;
	FitingDirPageContent *dir;
	FitingDirPageContent *empty_dir;
	int					 total_data_pages;
	/* Build-time overflow pool (palloc'd; no disk I/O during Phase 4) */
	FitingPageListNode	*build_overflow = NULL;
	int					 build_ov_alloc = 0;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "fiting_tree index \"%s\" already contains data",
			 RelationGetRelationName(index));

	/* Initialise build state — read per-index options if present */
	{
		FitingOptions *opts = FitingGetOptions(index);

		memset(&bs, 0, sizeof(bs));
		bs.max_error = opts ? opts->max_error : FITING_DEFAULT_MAX_ERROR;
	}
	bs.keytype    = TupleDescAttr(RelationGetDescr(index), 0)->atttypid;
	bs.alloc_size = 4096;
	bs.tuples     = palloc(bs.alloc_size * sizeof(FitingLeafTuple));
	bs.tmpCtx     = AllocSetContextCreate(CurrentMemoryContext,
										  "fiting_tree build tmp",
										  ALLOCSET_DEFAULT_SIZES);

	/* ---- Phase 1: heap scan ------------------------------------------ */
	reltuples = table_index_build_scan(heap, index, indexInfo,
									   true, true,
									   fiting_build_callback,
									   &bs, NULL);

	MemoryContextDelete(bs.tmpCtx);

	counts = palloc0(FITING_META_MAX_PAGES * sizeof(int32));

	/* Handle empty table */
	if (bs.ntups == 0)
	{
		memset(&meta, 0, sizeof(meta));
		meta.magic             = FITING_MAGIC;
		meta.max_error         = bs.max_error;
		meta.total_tuples      = 0;
		meta.num_segments      = 0;
		meta.dir_blkno         = InvalidBlockNumber;
		meta.freelist_head     = InvalidBlockNumber;
		meta.num_tracked_pages = 0;
		meta.buf_curr_page     = InvalidBlockNumber;
		meta.buf_curr_slots    = 0;
		meta.data_curr_page    = InvalidBlockNumber;
		meta.data_curr_fill    = 0;

		/* Block 0: meta */
		write_page_to_index(index, FITING_F_META, NULL, 0);
		fiting_write_meta_and_counts(index, &meta, counts);

		/*
		 * Block 1: empty directory page.
		 * Must always be present so that scans and inserts can ReadBuffer(1)
		 * without hitting "read only 0 of 8192 bytes".
		 */
		empty_dir = palloc0(sizeof(FitingDirPageContent));
		empty_dir->hdr.num_segments  = 0;
		empty_dir->hdr.pool_size     = 0;
		empty_dir->hdr.pool_freelist = -1;
		empty_dir->hdr.next_page     = InvalidBlockNumber;
		write_page_to_index(index, FITING_F_DIR, NULL, 0);
		fiting_write_dir_page(index, empty_dir);
		pfree(empty_dir);

		pfree(bs.tuples);
		pfree(counts);
		result = palloc(sizeof(IndexBuildResult));
		result->heap_tuples  = reltuples;
		result->index_tuples = 0;
		return result;
	}

	/* ---- Phase 2: sort ------------------------------------------------ */
	qsort(bs.tuples, (size_t) bs.ntups, sizeof(FitingLeafTuple),
		  leaf_tuple_cmp);

	/* ---- Phase 3: ShrinkingCone --------------------------------------- */
	keys = palloc(bs.ntups * sizeof(int64));
	for (int64 i = 0; i < bs.ntups; i++)
		keys[i] = bs.tuples[i].key;

	segs = FitingRunShrinkingCone(keys, bs.ntups, bs.max_error, &num_segs);
	pfree(keys);

	/* ---- Phase 3b: classify ShrinkingCone segments into FinalEntry[] ---- */
	{
		FitingOptions *opts        = FitingGetOptions(index);
		int64		   btree_window = opts ? opts->btree_window_size
										 : FITING_BTREE_WINDOW;
		int			   btree_thresh = opts ? opts->btree_seg_thresh
										 : FITING_BTREE_SEG_THRESH;
		int64	   *seg_sizes = palloc(num_segs * sizeof(int64));
		FinalEntry *finals;
		int			nfinals;
		int			f;

		for (int i = 0; i < num_segs; i++)
			seg_sizes[i] = (i + 1 < num_segs)
						   ? segs[i + 1].base_rank - segs[i].base_rank
						   : bs.ntups - segs[i].base_rank;

		finals = classify_segments(segs, seg_sizes, num_segs,
								   btree_window, btree_thresh, &nfinals);
		pfree(seg_sizes);

		if (nfinals > FITING_DIR_MAX_SEGS)
			elog(ERROR,
				 "fiting_tree: too many directory entries (%d, max %d). "
				 "Increase max_error or reduce data non-linearity.",
				 nfinals, FITING_DIR_MAX_SEGS);

		elog(LOG, "fiting_tree build: %lld tuples → %d raw segs → %d dir entries "
			 "(max_error=%d, avg seg len=%.1f)",
			 (long long) bs.ntups, num_segs, nfinals, bs.max_error,
			 (double) bs.ntups / nfinals);

		/* ---- Phase 4: build directory entries + node lists ------------- */
		total_data_pages = (int) ((bs.ntups + FITING_TUPLES_PER_PAGE - 1)
								  / FITING_TUPLES_PER_PAGE);

		dir = palloc0(sizeof(FitingDirPageContent));
		dir->hdr.num_segments  = nfinals;
		dir->hdr.pool_size     = 0;
		dir->hdr.pool_freelist = -1;
		dir->hdr.next_page     = InvalidBlockNumber;

		for (f = 0; f < nfinals; f++)
		{
			FinalEntry *fe         = &finals[f];
			int64		base       = segs[fe->from_seg].base_rank;
			int64		seg_end    = (fe->to_seg + 1 < num_segs)
								   ? segs[fe->to_seg + 1].base_rank
								   : bs.ntups;
			int64		sz         = fe->total_tuples;
			int			start_slot = (int) (base % FITING_TUPLES_PER_PAGE);
			int			first_pg   = (int) (base / FITING_TUPLES_PER_PAGE);
			int			last_pg    = (int) ((seg_end - 1) / FITING_TUPLES_PER_PAGE);
			int			prev_node  = -1;
			int			list_head  = -1;
			int			p;

			/* Allocate one node per page this entry spans */
			for (p = first_pg; p <= last_pg; p++)
			{
				int		node_idx = build_alloc_node(dir, &build_overflow,
													 &build_ov_alloc);
				/* page_start_key: first key of THIS entry's data on page p */
				int64	pfirst   = (p == first_pg)
								 ? base
								 : (int64) p * FITING_TUPLES_PER_PAGE;
				FitingPageListNode *nd = BUILD_NODE(node_idx, dir, build_overflow);

				nd->page_no        = FITING_DATA_START_BLKNO + p;
				nd->next           = -1;
				nd->page_start_key = bs.tuples[pfirst].key;

				if (prev_node >= 0)
					BUILD_NODE(prev_node, dir, build_overflow)->next = node_idx;
				else
					list_head = node_idx;
				prev_node = node_idx;

				/* Increment page ref-count (shared boundary pages get 2) */
				if (p < FITING_META_MAX_PAGES)
					counts[p]++;
			}

			dir->entries[f].start_key         = segs[fe->from_seg].start_key;
			dir->entries[f].slope             = (fe->seg_type == FITING_SEG_TYPE_FITING)
												? segs[fe->from_seg].slope : 0.0;
			dir->entries[f].page_list_head    = list_head;
			dir->entries[f].seg_total_tuples  = (int32) sz;
			dir->entries[f].seg_info          = 0;
			FitingSegSetType(&dir->entries[f], fe->seg_type);
			dir->entries[f].buf_blkno  = InvalidBlockNumber;
			dir->entries[f].buf_info   = 0;
			dir->entries[f].start_slot = start_slot;
		}

		num_segs = nfinals;		/* update so meta.num_segments is set correctly */
		pfree(finals);
	}

	/* ---- Phase 5: write index pages ---------------------------------- */
	memset(&meta, 0, sizeof(meta));
	meta.magic             = FITING_MAGIC;
	meta.max_error         = bs.max_error;
	meta.total_tuples      = bs.ntups;
	meta.num_segments      = num_segs;
	meta.dir_blkno         = FITING_DIR_BLKNO;
	meta.freelist_head     = InvalidBlockNumber;
	meta.num_tracked_pages = total_data_pages;
	meta.buf_curr_page     = InvalidBlockNumber;
	meta.buf_curr_slots    = 0;
	/*
	 * data_curr_page / data_curr_fill: track the last data page so that
	 * subsequent inserts → resegments can append to it rather than always
	 * allocating a fresh page.
	 */
	{
		int64 last_fill = bs.ntups % FITING_TUPLES_PER_PAGE;

		if (last_fill == 0 || total_data_pages == 0)
		{
			meta.data_curr_page = InvalidBlockNumber;
			meta.data_curr_fill = 0;
		}
		else
		{
			meta.data_curr_page = FITING_DATA_START_BLKNO + total_data_pages - 1;
			meta.data_curr_fill = (int32) last_fill;
		}
	}

	/* Block 0: placeholder meta (extend file to correct block) */
	write_page_to_index(index, FITING_F_META, NULL, 0);

	/* Block 1: directory */
	write_page_to_index(index, FITING_F_DIR, NULL, 0);

	/* Blocks 2+: leaf data, packed globally (same byte layout as before) */
	{
		int64 written = 0;

		while (written < bs.ntups)
		{
			int64 batch = bs.ntups - written;

			if (batch > FITING_TUPLES_PER_PAGE)
				batch = FITING_TUPLES_PER_PAGE;

			write_page_to_index(index, FITING_F_LEAF,
								bs.tuples + written,
								(size_t) batch * sizeof(FitingLeafTuple));
			written += batch;
		}
	}

	/*
	 * Flush build-time overflow nodes to disk.
	 *
	 * During Phase 4 we kept overflow nodes (pool_size > FITING_DIR_MAX_NODES)
	 * in the palloc'd build_overflow array to avoid disk I/O before data pages
	 * existed.  Now that data pages have been written, we extend the relation
	 * further with overflow dir pages, copy nodes into them, link the chain,
	 * and record dir->hdr.next_page so fiting_write_dir_page persists it.
	 */
	if (dir->hdr.pool_size > FITING_DIR_MAX_NODES && build_overflow != NULL)
	{
		int ov_count = dir->hdr.pool_size - FITING_DIR_MAX_NODES;
		int ov_pages = (ov_count + FITING_DIR_OVERFLOW_NODES - 1)
					   / FITING_DIR_OVERFLOW_NODES;
		BlockNumber *ov_blknos = palloc(ov_pages * sizeof(BlockNumber));
		int op;

		/* First pass: write each overflow page with its node data */
		for (op = 0; op < ov_pages; op++)
		{
			int				ov_start = op * FITING_DIR_OVERFLOW_NODES;
			int				ov_n     = ov_count - ov_start;
			Buffer			buf;
			Page			page;
			GenericXLogState *xstate;
			FitingDirOverflowHeader *ohdr;
			FitingPageListNode		*pool_area;
			BlockNumber		blkno;
			int				cidx;

			if (ov_n > FITING_DIR_OVERFLOW_NODES)
				ov_n = FITING_DIR_OVERFLOW_NODES;

			buf   = ExtendBufferedRel(BMR_REL(index), MAIN_FORKNUM, NULL,
									  EB_LOCK_FIRST);
			blkno = BufferGetBlockNumber(buf);
			ov_blknos[op] = blkno;

			/* Track in ref-count array */
			cidx = (int) (blkno - FITING_DATA_START_BLKNO);
			if (cidx >= 0 && cidx < FITING_META_MAX_PAGES)
			{
				counts[cidx] = 1;
				if (cidx >= meta.num_tracked_pages)
					meta.num_tracked_pages = cidx + 1;
			}

			xstate    = GenericXLogStart(index);
			page      = GenericXLogRegisterBuffer(xstate, buf,
												   GENERIC_XLOG_FULL_IMAGE);
			FitingInitPage(page, FITING_F_DIR);
			ohdr            = (FitingDirOverflowHeader *) PageGetContents(page);
			ohdr->next_page = InvalidBlockNumber;	/* linked in second pass */
			ohdr->pad       = 0;
			pool_area       = (FitingPageListNode *)(ohdr + 1);
			memset(pool_area, 0,
				   FITING_DIR_OVERFLOW_NODES * sizeof(FitingPageListNode));
			memcpy(pool_area, build_overflow + ov_start,
				   ov_n * sizeof(FitingPageListNode));
			((PageHeader) page)->pd_lower +=
				(int) (sizeof(FitingDirOverflowHeader) +
					   FITING_DIR_OVERFLOW_NODES * sizeof(FitingPageListNode));
			GenericXLogFinish(xstate);
			UnlockReleaseBuffer(buf);
		}

		/* Second pass: link the chain and update dir->hdr.next_page */
		dir->hdr.next_page = ov_blknos[0];
		for (op = 0; op < ov_pages - 1; op++)
		{
			Buffer			 buf;
			Page			 page;
			GenericXLogState *xstate;

			buf    = ReadBuffer(index, ov_blknos[op]);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			xstate = GenericXLogStart(index);
			page   = GenericXLogRegisterBuffer(xstate, buf, 0);
			((FitingDirOverflowHeader *) PageGetContents(page))->next_page =
				ov_blknos[op + 1];
			GenericXLogFinish(xstate);
			UnlockReleaseBuffer(buf);
		}

		pfree(ov_blknos);
		pfree(build_overflow);
	}

	/* Overwrite block 0 with real meta + counts */
	fiting_write_meta_and_counts(index, &meta, counts);

	/* Overwrite block 1 with directory (header + entries + pool) */
	fiting_write_dir_page(index, dir);

	pfree(bs.tuples);
	pfree(segs);
	pfree(counts);
	pfree(dir);

	result = palloc(sizeof(IndexBuildResult));
	result->heap_tuples  = reltuples;
	result->index_tuples = (double) bs.ntups;
	return result;
}

/* -----------------------------------------------------------------------
 * fiting_buildempty — for unlogged tables (INIT_FORKNUM)
 * ----------------------------------------------------------------------- */
void
fiting_buildempty(Relation index)
{
	Buffer				buf;
	Page				page;
	GenericXLogState   *xstate;
	FitingMetaPageData	meta;

	buf = ReadBufferExtended(index, INIT_FORKNUM, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	Assert(BufferGetBlockNumber(buf) == FITING_META_BLKNO);

	xstate = GenericXLogStart(index);
	page   = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

	FitingInitPage(page, FITING_F_META);

	memset(&meta, 0, sizeof(meta));
	meta.magic             = FITING_MAGIC;
	meta.max_error         = FITING_DEFAULT_MAX_ERROR;
	meta.total_tuples      = 0;
	meta.num_segments      = 0;
	meta.dir_blkno         = InvalidBlockNumber;
	meta.freelist_head     = InvalidBlockNumber;
	meta.num_tracked_pages = 0;
	meta.buf_curr_page     = InvalidBlockNumber;
	meta.buf_curr_slots    = 0;
	meta.data_curr_page    = InvalidBlockNumber;
	meta.data_curr_fill    = 0;

	memcpy(PageGetContents(page), &meta, sizeof(meta));
	((PageHeader) page)->pd_lower += sizeof(meta);

	GenericXLogFinish(xstate);
	UnlockReleaseBuffer(buf);
}

/* =======================================================================
 * Shared page-management helpers
 * ======================================================================= */

/* -----------------------------------------------------------------------
 * fiting_alloc_page
 *
 * Return a block number ready for use.  Pops from meta->freelist_head
 * first; falls back to ExtendBufferedRel.
 *
 * Sets counts[blkno − DATA_START] = 1 for the allocated page.
 * Caller must write meta + counts back to disk afterward.
 * ----------------------------------------------------------------------- */
BlockNumber
fiting_alloc_page(Relation index, FitingMetaPageData *meta, int32 *counts)
{
	BlockNumber blkno;
	int			idx;

	if (meta->freelist_head != InvalidBlockNumber)
	{
		Buffer		buf;
		Page		page;
		BlockNumber	next;

		blkno = meta->freelist_head;

		buf  = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		next = FitingFreePage_GetNext(page);
		UnlockReleaseBuffer(buf);

		meta->freelist_head = next;
	}
	else
	{
		Buffer buf;

		buf   = ExtendBufferedRel(BMR_REL(index), MAIN_FORKNUM, NULL,
								  EB_LOCK_FIRST);
		blkno = BufferGetBlockNumber(buf);
		UnlockReleaseBuffer(buf);
	}

	/* Set ref-count to 1 for the newly allocated page */
	idx = (int) (blkno - FITING_DATA_START_BLKNO);
	if (idx >= 0 && idx < FITING_META_MAX_PAGES)
	{
		counts[idx] = 1;
		if (idx >= meta->num_tracked_pages)
			meta->num_tracked_pages = idx + 1;
	}

	return blkno;
}

/* -----------------------------------------------------------------------
 * fiting_free_page
 *
 * Ref-count aware page free.
 *
 * Decrements counts[blkno − DATA_START].  If the count is still > 0 after
 * decrement (i.e. another segment still references this page), the block is
 * NOT wiped — only the count is updated.  This is the fix for the
 * shared-page bug: the initial bulk build can leave two segments sharing
 * one page; decrement-only protects the still-live segment's tail data.
 *
 * When the count reaches 0 (or the block is outside the tracked range),
 * the page is physically freed: wiped with FITING_F_FREE and prepended to
 * meta->freelist_head.
 *
 * Caller must write meta + counts back to disk afterward.
 * ----------------------------------------------------------------------- */
void
fiting_free_page(Relation index, FitingMetaPageData *meta, int32 *counts,
				  BlockNumber blkno)
{
	int				idx;
	Buffer			buf;
	Page			page;
	GenericXLogState *xstate;

	idx = (int) (blkno - FITING_DATA_START_BLKNO);

	if (idx >= 0 && idx < FITING_META_MAX_PAGES && counts[idx] > 0)
	{
		counts[idx]--;
		if (counts[idx] > 0)
			return;		/* page still referenced by another segment */
	}

	/*
	 * If the page being freed was the current open data or buffer page,
	 * invalidate those pointers so we don't try to append to a freed page.
	 */
	if (blkno == meta->buf_curr_page)
		meta->buf_curr_page = InvalidBlockNumber;
	if (blkno == meta->data_curr_page)
		meta->data_curr_page = InvalidBlockNumber;

	/* Actually free the page */
	buf = ReadBuffer(index, blkno);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	xstate = GenericXLogStart(index);
	page   = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

	FitingInitPage(page, FITING_F_FREE);
	FitingFreePage_GetNext(page) = meta->freelist_head;
	((PageHeader) page)->pd_lower += sizeof(BlockNumber);

	GenericXLogFinish(xstate);
	UnlockReleaseBuffer(buf);

	meta->freelist_head = blkno;
}

/* -----------------------------------------------------------------------
 * fiting_alloc_buf_slot
 *
 * Assign a 32-tuple buffer slot to seg.  Up to FITING_BUF_SLOTS_PER_PAGE
 * (15) segments share one 8 kB buffer page.  When the current shared page
 * is full (or does not yet exist), a new page is allocated via
 * fiting_alloc_page, zeroed, and registered as meta->buf_curr_page.
 *
 * On return, seg->buf_blkno and seg->buf_info are set; the slot count
 * within buf_info is 0 (no tuples yet).  meta and counts are updated in
 * memory; caller must persist them.
 * ----------------------------------------------------------------------- */
void
fiting_alloc_buf_slot(Relation index,
					  FitingMetaPageData *meta,
					  int32 *counts,
					  FitingDirEntry *seg)
{
	int			slot_idx;
	BlockNumber	blkno;

	if (meta->buf_curr_page == InvalidBlockNumber ||
		meta->buf_curr_slots >= FITING_BUF_SLOTS_PER_PAGE)
	{
		/*
		 * Current buffer page is full or does not exist.
		 * Allocate a fresh page and initialise all slots to zero.
		 */
		Buffer			   buf;
		Page			   page;
		GenericXLogState  *xstate;

		blkno = fiting_alloc_page(index, meta, counts);

		buf    = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		xstate = GenericXLogStart(index);
		page   = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

		FitingInitPage(page, FITING_F_BUF);
		/* Zero the entire slot area so stale data cannot leak */
		memset(PageGetContents(page), 0,
			   FITING_BUF_SLOTS_PER_PAGE *
			   FITING_BUF_TUPLES_PER_SEG * sizeof(FitingLeafTuple));
		((PageHeader) page)->pd_lower +=
			FITING_BUF_SLOTS_PER_PAGE *
			FITING_BUF_TUPLES_PER_SEG * sizeof(FitingLeafTuple);

		GenericXLogFinish(xstate);
		UnlockReleaseBuffer(buf);

		meta->buf_curr_page  = blkno;
		meta->buf_curr_slots = 1;
		slot_idx             = 0;
	}
	else
	{
		/* Reuse the current page — bump its ref-count for the new segment */
		int cidx;

		blkno    = meta->buf_curr_page;
		slot_idx = meta->buf_curr_slots;

		cidx = (int) (blkno - FITING_DATA_START_BLKNO);
		if (cidx >= 0 && cidx < FITING_META_MAX_PAGES)
			counts[cidx]++;

		meta->buf_curr_slots++;
	}

	seg->buf_blkno = blkno;
	FitingBufSetInfo(seg, slot_idx, 0);
}

/* -----------------------------------------------------------------------
 * fiting_resegment
 *
 * Re-segment dir->entries[seg_idx]:
 *   1. Collect live (non-deleted) tuples from the segment's data pages
 *      by walking its page linked list.
 *   2. Merge with buffer-slot tuples (slot-offset aware, always live).
 *   3. Run ShrinkingCone on the merged sorted array.
 *   4. Collect old page block numbers; free dir-pool nodes.
 *      Release old buffer slot (ref-count decrement).
 *   5. Write new segment pages using in-place reuse: overwrite old pages
 *      first (full page, count set to 1), then append to data_curr_page,
 *      then allocate from freelist.  Free any surplus old pages afterward.
 *   6. Splice new FitingDirEntry records into dir->entries[]; update meta.
 *   7. Write directory and meta+counts pages.
 *
 * meta, counts, and dir are writable in-memory copies.  All three are
 * updated in memory AND written to disk before returning.
 * ----------------------------------------------------------------------- */
void
fiting_resegment(Relation index, FitingMetaPageData *meta, int32 *counts,
				 FitingDirPageContent *dir, int seg_idx)
{
	FitingDirEntry	   *old_seg = &dir->entries[seg_idx];
	FitingLeafTuple    *live_data;
	int64				live_count   = 0;
	int64				dead_seen    = 0;
	FitingLeafTuple    *buf_data    = NULL;
	int32				num_buf      = FitingBufCount(old_seg);
	FitingLeafTuple    *merged;
	int64				merged_count;
	int64			   *keys;
	FitingSegment	   *new_segs;
	int					new_num_segs;
	int					total_new_capacity;
	FitingDirEntry	   *new_entries;
	int					old_page_list_head;
	BlockNumber			old_buf_blkno;
	BlockNumber		   *old_page_list = NULL;
	int					num_old_pages  = 0;

	/* ---- Step 1: collect live tuples from data pages (walk node list) -- */
	live_data = palloc((old_seg->seg_total_tuples + num_buf)
					   * sizeof(FitingLeafTuple));

	{
		int		node_idx       = old_seg->page_list_head;
		int		page_local_num = 0;

		while (node_idx >= 0)
		{
			FitingPageListNode nd;
			int				next_node_idx;
			Buffer			buf;
			Page			page;
			FitingLeafTuple *page_tuples;
			int				n_on_page;
			int				start_slot;
			int				s;

			fiting_get_node(index, dir, node_idx, &nd);
			next_node_idx = nd.next;

			buf        = ReadBuffer(index, nd.page_no);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page       = BufferGetPage(buf);
			page_tuples = FitingPageGetLeaf(page);
			n_on_page  = (int) (((PageHeader) page)->pd_lower
								- SizeOfPageHeaderData)
						 / (int) sizeof(FitingLeafTuple);

			/* First page of segment: skip slots owned by the previous seg */
			start_slot = (page_local_num == 0) ? old_seg->start_slot : 0;

			for (s = start_slot; s < n_on_page; s++)
			{
				/* Stop once we've accounted for all tuples in this segment */
				if (live_count + dead_seen >= old_seg->seg_total_tuples)
					break;

				if (page_tuples[s].flags & FITING_LEAF_DELETED)
					dead_seen++;
				else
					live_data[live_count++] = page_tuples[s];
			}

			UnlockReleaseBuffer(buf);

			if (live_count + dead_seen >= old_seg->seg_total_tuples)
				break;

			node_idx = next_node_idx;
			page_local_num++;
		}
	}

	/* ---- Step 2: load buffer entries (always live) ------------------- */
	if (old_seg->buf_blkno != InvalidBlockNumber && num_buf > 0)
	{
		Buffer	buf;
		Page	page;

		buf_data = palloc(num_buf * sizeof(FitingLeafTuple));

		buf = ReadBuffer(index, old_seg->buf_blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		/* Read from the correct slot offset within the shared buffer page */
		memcpy(buf_data,
			   FitingPageGetLeaf(page) +
			   FitingBufSlot(old_seg) * FITING_BUF_TUPLES_PER_SEG,
			   num_buf * sizeof(FitingLeafTuple));
		UnlockReleaseBuffer(buf);
	}

	/* ---- Step 3: merge live_data + buf_data (both sorted) ------------ */
	merged_count = live_count + num_buf;
	merged       = palloc(merged_count > 0 ? merged_count * sizeof(FitingLeafTuple)
										   : sizeof(FitingLeafTuple));

	{
		int64 i = 0, j = 0, k = 0;

		while (i < live_count && j < num_buf)
		{
			if (live_data[i].key <= buf_data[j].key)
				merged[k++] = live_data[i++];
			else
				merged[k++] = buf_data[j++];
		}
		while (i < live_count)
			merged[k++] = live_data[i++];
		while (j < num_buf)
			merged[k++] = buf_data[j++];
	}

	pfree(live_data);
	if (buf_data)
		pfree(buf_data);

	/* Save old list head and buf_blkno before we modify the entry */
	old_page_list_head = old_seg->page_list_head;
	old_buf_blkno      = old_seg->buf_blkno;

	/*
	 * ---- Step 4: collect old data pages into a reuse pool ---------------
	 *
	 * We walk the old segment's linked list and gather every page block
	 * number.  Dir-pool nodes are freed immediately (reclaimed for new
	 * segments).  The actual disk pages are NOT freed here — we will
	 * overwrite them in-place during Step 5 (full reuse), and only call
	 * fiting_free_page for pages that end up surplus after the write.
	 *
	 * For shared boundary pages (counts > 1, set during the initial bulk
	 * build): we still overwrite them fully and set counts = 1 to take
	 * exclusive ownership.  The neighbouring segment's reference to that
	 * page becomes stale; it will be corrected when that segment is itself
	 * resegmented.
	 */
	{
		int  node_idx  = old_page_list_head;
		int  old_count = 0;

		/* Count pages first */
		while (node_idx >= 0)
		{
			FitingPageListNode nd;

			fiting_get_node(index, dir, node_idx, &nd);
			old_count++;
			node_idx = nd.next;
		}

		old_page_list = palloc(old_count > 0
							   ? old_count * sizeof(BlockNumber)
							   : sizeof(BlockNumber));
		num_old_pages = 0;

		node_idx = old_page_list_head;
		while (node_idx >= 0)
		{
			FitingPageListNode nd;
			int next;

			fiting_get_node(index, dir, node_idx, &nd);
			next = nd.next;
			old_page_list[num_old_pages++] = nd.page_no;

			/* Inline free: push onto freelist */
			nd.page_no        = InvalidBlockNumber;
			nd.page_start_key = 0;
			nd.next           = dir->hdr.pool_freelist;
			fiting_put_node(index, dir, node_idx, &nd);
			dir->hdr.pool_freelist = node_idx;

			node_idx = next;
		}
	}

	/* Release old buffer slot (ref-count decrement only for shared page) */
	if (old_buf_blkno != InvalidBlockNumber)
	{
		fiting_free_page(index, meta, counts, old_buf_blkno);
		old_seg->buf_info = 0;
	}

	/* Segment fully dead + no live buffer — remove from directory */
	if (merged_count == 0)
	{
		int ri;

		for (ri = 0; ri < num_old_pages; ri++)
			fiting_free_page(index, meta, counts, old_page_list[ri]);
		pfree(old_page_list);

		meta->num_segments--;
		dir->hdr.num_segments--;

		memmove(&dir->entries[seg_idx],
				&dir->entries[seg_idx + 1],
				(meta->num_segments - seg_idx) * sizeof(FitingDirEntry));
		memset(&dir->entries[meta->num_segments], 0, sizeof(FitingDirEntry));

		pfree(merged);
		goto write_dir_meta;
	}

	/* Adjust total live count for tombstones that were purged */
	meta->total_tuples -= dead_seen;

	/* ---- Step 4b: ShrinkingCone + classify on merged array ----------- */
	keys = palloc(merged_count * sizeof(int64));
	for (int64 i = 0; i < merged_count; i++)
		keys[i] = merged[i].key;

	new_segs = FitingRunShrinkingCone(keys, merged_count,
									  meta->max_error, &new_num_segs);
	pfree(keys);

	/* Classify raw ShrinkingCone output into FITING / BTREE FinalEntries */
	{
		FitingOptions *opts        = FitingGetOptions(index);
		int64		   btree_window = opts ? opts->btree_window_size
										 : FITING_BTREE_WINDOW;
		int			   btree_thresh = opts ? opts->btree_seg_thresh
										 : FITING_BTREE_SEG_THRESH;
		int64	   *seg_sizes = palloc(new_num_segs * sizeof(int64));
		FinalEntry *finals;
		int			nfinals;
		int			f;
		int			reuse_idx;		/* next old page to reuse in-place */

		for (int i = 0; i < new_num_segs; i++)
			seg_sizes[i] = (i + 1 < new_num_segs)
						   ? new_segs[i + 1].base_rank - new_segs[i].base_rank
						   : merged_count - new_segs[i].base_rank;

		finals = classify_segments(new_segs, seg_sizes, new_num_segs,
								   btree_window, btree_thresh, &nfinals);
		pfree(seg_sizes);

		/* Validate we still fit in one directory page */
		total_new_capacity = meta->num_segments - 1 + nfinals;
		if (total_new_capacity > FITING_DIR_MAX_SEGS)
			elog(ERROR,
				 "fiting_tree: re-segmentation would produce %d total segments "
				 "(max %d). Increase max_error.",
				 total_new_capacity, FITING_DIR_MAX_SEGS);

		/* ---- Step 5: write new segment pages (in-place reuse first) ---- *
		 *                                                                    *
		 * Priority order for each new page needed:                          *
		 *   1. Reuse an old page in-place (full overwrite, count set to 1). *
		 *   2. Append to meta->data_curr_page if it has capacity.           *
		 *   3. Allocate a fresh page from the freelist / extend the file.   *
		 *                                                                    *
		 * After writing all new segments, leftover old pages are freed.     */
		new_entries = palloc0(nfinals * sizeof(FitingDirEntry));
		reuse_idx   = 0;

		for (f = 0; f < nfinals; f++)
		{
			FinalEntry *fe        = &finals[f];
			int64		seg_start = new_segs[fe->from_seg].base_rank;
			int64		seg_size  = fe->total_tuples;
			int64		written   = 0;
			int			prev_node = -1;

			new_entries[f].start_key        = new_segs[fe->from_seg].start_key;
			new_entries[f].slope            = (fe->seg_type == FITING_SEG_TYPE_FITING)
											  ? new_segs[fe->from_seg].slope : 0.0;
			new_entries[f].seg_total_tuples = (int32) seg_size;
			new_entries[f].seg_info         = 0;
			FitingSegSetType(&new_entries[f], fe->seg_type);
			new_entries[f].buf_blkno        = InvalidBlockNumber;
			new_entries[f].buf_info         = 0;
			new_entries[f].start_slot       = 0;
			new_entries[f].page_list_head   = -1;

			while (written < seg_size)
			{
				int64				batch          = seg_size - written;
				BlockNumber			blkno;
				int					page_start_slot = 0;
				Buffer				wbuf;
				Page				wpage;
				GenericXLogState   *xstate;
				int					node_idx;

				if (batch > FITING_TUPLES_PER_PAGE)
					batch = FITING_TUPLES_PER_PAGE;

				if (reuse_idx < num_old_pages)
				{
					/*
					 * Reuse an old page in-place.  Take exclusive ownership
					 * (set count = 1) regardless of whether it was shared.
					 */
					int cidx;

					blkno = old_page_list[reuse_idx++];
					cidx  = (int) (blkno - FITING_DATA_START_BLKNO);
					if (cidx >= 0 && cidx < FITING_META_MAX_PAGES)
						counts[cidx] = 1;

					page_start_slot = 0;

					wbuf   = ReadBuffer(index, blkno);
					LockBuffer(wbuf, BUFFER_LOCK_EXCLUSIVE);
					xstate = GenericXLogStart(index);
					wpage  = GenericXLogRegisterBuffer(xstate, wbuf,
													   GENERIC_XLOG_FULL_IMAGE);
					FitingInitPage(wpage, FITING_F_LEAF);
					memcpy(PageGetContents(wpage),
						   merged + seg_start + written,
						   (size_t) batch * sizeof(FitingLeafTuple));
					((PageHeader) wpage)->pd_lower +=
						(int) batch * sizeof(FitingLeafTuple);
					GenericXLogFinish(xstate);
					UnlockReleaseBuffer(wbuf);
				}
				else if (meta->data_curr_page != InvalidBlockNumber &&
						 meta->data_curr_fill > 0 &&
						 meta->data_curr_fill < FITING_TUPLES_PER_PAGE)
				{
					/*
					 * Append to the current partial data page.
					 * Limit batch to what fits in the remaining slots.
					 */
					int avail = FITING_TUPLES_PER_PAGE - meta->data_curr_fill;
					int cidx;

					if (batch > avail)
						batch = avail;

					blkno           = meta->data_curr_page;
					page_start_slot = meta->data_curr_fill;

					cidx = (int) (blkno - FITING_DATA_START_BLKNO);
					if (cidx >= 0 && cidx < FITING_META_MAX_PAGES)
						counts[cidx]++;	/* one more segment references this page */

					/*
					 * Use flag 0 (keep existing page content) so we don't
					 * clobber data written by the previous segment.
					 */
					wbuf   = ReadBuffer(index, blkno);
					LockBuffer(wbuf, BUFFER_LOCK_EXCLUSIVE);
					xstate = GenericXLogStart(index);
					wpage  = GenericXLogRegisterBuffer(xstate, wbuf, 0);
					memcpy((char *) PageGetContents(wpage) +
						   page_start_slot * sizeof(FitingLeafTuple),
						   merged + seg_start + written,
						   (size_t) batch * sizeof(FitingLeafTuple));
					((PageHeader) wpage)->pd_lower =
						SizeOfPageHeaderData +
						(page_start_slot + (int) batch) * sizeof(FitingLeafTuple);
					GenericXLogFinish(xstate);
					UnlockReleaseBuffer(wbuf);
				}
				else
				{
					/* Allocate a fresh page from the freelist */
					blkno           = fiting_alloc_page(index, meta, counts);
					page_start_slot = 0;

					wbuf   = ReadBuffer(index, blkno);
					LockBuffer(wbuf, BUFFER_LOCK_EXCLUSIVE);
					xstate = GenericXLogStart(index);
					wpage  = GenericXLogRegisterBuffer(xstate, wbuf,
													   GENERIC_XLOG_FULL_IMAGE);
					FitingInitPage(wpage, FITING_F_LEAF);
					memcpy(PageGetContents(wpage),
						   merged + seg_start + written,
						   (size_t) batch * sizeof(FitingLeafTuple));
					((PageHeader) wpage)->pd_lower +=
						(int) batch * sizeof(FitingLeafTuple);
					GenericXLogFinish(xstate);
					UnlockReleaseBuffer(wbuf);
				}

				/* Update data_curr_page / data_curr_fill */
				{
					int new_fill = page_start_slot + (int) batch;

					if (new_fill >= FITING_TUPLES_PER_PAGE)
					{
						if (blkno == meta->data_curr_page)
						{
							meta->data_curr_page = InvalidBlockNumber;
							meta->data_curr_fill = 0;
						}
					}
					else
					{
						meta->data_curr_page = blkno;
						meta->data_curr_fill = new_fill;
					}
				}

				/* Record start_slot for the first page of this segment */
				if (written == 0)
					new_entries[f].start_slot = page_start_slot;

				/* Append a node to this segment's linked list */
				{
					FitingPageListNode new_nd;

					new_nd.page_no        = blkno;
					new_nd.next           = -1;
					new_nd.page_start_key =
						merged[seg_start + written].key;

					node_idx = fiting_dir_alloc_node(dir, index, meta, counts);
					fiting_put_node(index, dir, node_idx, &new_nd);

					if (prev_node >= 0)
					{
						FitingPageListNode prev_nd;

						fiting_get_node(index, dir, prev_node, &prev_nd);
						prev_nd.next = node_idx;
						fiting_put_node(index, dir, prev_node, &prev_nd);
					}
					else
						new_entries[f].page_list_head = node_idx;
					prev_node = node_idx;
				}

				written += batch;
			}
		}

		/* Free any old pages that were not needed for the new segments */
		for (; reuse_idx < num_old_pages; reuse_idx++)
			fiting_free_page(index, meta, counts, old_page_list[reuse_idx]);
		pfree(old_page_list);

		pfree(finals);
		pfree(merged);
		pfree(new_segs);

		/* ---- Step 6: splice new entries into dir ----------------------- */
		if (nfinals == 1)
		{
			dir->entries[seg_idx] = new_entries[0];
		}
		else
		{
			int tail = meta->num_segments - seg_idx - 1;

			memmove(&dir->entries[seg_idx + nfinals],
					&dir->entries[seg_idx + 1],
					tail * sizeof(FitingDirEntry));

			for (f = 0; f < nfinals; f++)
				dir->entries[seg_idx + f] = new_entries[f];

			meta->num_segments   += nfinals - 1;
			dir->hdr.num_segments = meta->num_segments;
		}

		pfree(new_entries);
	}

	/* suppress unused-variable warning for new_num_segs after refactor */
	(void) new_num_segs;

write_dir_meta:
	/* ---- Step 7: write directory and meta+counts to disk ------------- */
	fiting_write_dir_page(index, dir);
	fiting_write_meta_and_counts(index, meta, counts);
}
