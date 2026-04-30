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
 *   3. fiting_resegment() frees old pages BEFORE allocating new ones
 *      ("free first, allocate next").  Freed blocks re-enter the freelist
 *      and can be reused immediately, avoiding unnecessary file extension.
 *
 *   4. fiting_write_seg_pages() now allocates via fiting_alloc_page()
 *      (freelist-first) instead of always using ExtendBufferedRel.
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

/* -----------------------------------------------------------------------
 * overwrite_page
 *
 * Overwrite block blkno in place under GenericXLog.
 * The block must already exist.
 * ----------------------------------------------------------------------- */
static void
overwrite_page(Relation index, BlockNumber blkno, uint16 page_flags,
			   const void *data, size_t data_bytes)
{
	Buffer				buf;
	Page				page;
	GenericXLogState   *xstate;

	buf = ReadBuffer(index, blkno);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

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
 * fiting_build
 * ======================================================================= */
IndexBuildResult *
fiting_build(Relation heap, Relation index, struct IndexInfo *indexInfo)
{
	FitingBuildState	bs;
	IndexBuildResult   *result;
	double				reltuples;
	int64			   *keys;
	FitingSegment	   *segs;
	int					num_segs;
	FitingMetaPageData	meta;
	int32			   *counts;
	FitingDirPageContent *dir;
	int					total_data_pages;

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
		meta.magic         = FITING_MAGIC;
		meta.max_error     = bs.max_error;
		meta.total_tuples  = 0;
		meta.num_segments  = 0;
		meta.dir_blkno     = InvalidBlockNumber;
		meta.freelist_head = InvalidBlockNumber;
		meta.num_tracked_pages = 0;

		FitingDirPageContent *empty_dir;

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
				int		node_idx = fiting_dir_alloc_node(dir);
				/* page_start_key: first key of THIS entry's data on page p */
				int64	pfirst   = (p == first_pg)
								 ? base
								 : (int64) p * FITING_TUPLES_PER_PAGE;

				dir->pool[node_idx].page_no        = FITING_DATA_START_BLKNO + p;
				dir->pool[node_idx].next           = -1;
				dir->pool[node_idx].page_start_key = bs.tuples[pfirst].key;

				if (prev_node >= 0)
					dir->pool[prev_node].next = node_idx;
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
			dir->entries[f].buf_blkno         = InvalidBlockNumber;
			dir->entries[f].num_buffer_tuples = 0;
			dir->entries[f].start_slot        = start_slot;
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
 * fiting_write_seg_pages
 *
 * Write tuples[0..ntups) to freshly allocated pages for entry.
 * Allocates each page via fiting_alloc_page (freelist-first), builds
 * the linked list of FitingPageListNode records in dir->pool, and sets
 * entry->page_list_head and entry->start_slot = 0.
 *
 * Caller must write meta + counts + dir back to disk afterward.
 * ----------------------------------------------------------------------- */
static void
fiting_write_seg_pages(Relation index,
					   FitingMetaPageData *meta,
					   int32 *counts,
					   FitingDirPageContent *dir,
					   FitingDirEntry *entry,
					   FitingLeafTuple *tuples, int64 ntups)
{
	int64	written  = 0;
	int		prev_node = -1;

	entry->start_slot       = 0;
	entry->page_list_head   = -1;

	while (written < ntups)
	{
		int64				batch = ntups - written;
		BlockNumber			blkno;
		Buffer				buf;
		Page				page;
		GenericXLogState   *xstate;
		int					node_idx;

		if (batch > FITING_TUPLES_PER_PAGE)
			batch = FITING_TUPLES_PER_PAGE;

		/* Allocate a page (freelist-first, extend only if needed) */
		blkno = fiting_alloc_page(index, meta, counts);

		/* Write tuples */
		buf    = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		xstate = GenericXLogStart(index);
		page   = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

		FitingInitPage(page, FITING_F_LEAF);
		memcpy(PageGetContents(page), tuples + written,
			   (size_t) batch * sizeof(FitingLeafTuple));
		((PageHeader) page)->pd_lower += (int) batch * sizeof(FitingLeafTuple);

		GenericXLogFinish(xstate);
		UnlockReleaseBuffer(buf);

		/* Append a node to this segment's linked list */
		node_idx = fiting_dir_alloc_node(dir);
		dir->pool[node_idx].page_no        = blkno;
		dir->pool[node_idx].next           = -1;
		dir->pool[node_idx].page_start_key = tuples[written].key;

		if (prev_node >= 0)
			dir->pool[prev_node].next = node_idx;
		else
			entry->page_list_head = node_idx;
		prev_node = node_idx;

		written += batch;
	}
}

/* -----------------------------------------------------------------------
 * fiting_resegment
 *
 * Re-segment dir->entries[seg_idx]:
 *   1. Collect live (non-deleted) tuples from the segment's data pages
 *      by walking its page linked list.
 *   2. Merge with buffer-page tuples (always live).
 *   3. Run ShrinkingCone on the merged sorted array.
 *   4. FREE OLD PAGES FIRST (ref-count aware) — freed pages go to freelist.
 *   5. Write new segment pages via fiting_write_seg_pages (freelist-first).
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
	int32				num_buf      = old_seg->num_buffer_tuples;
	FitingLeafTuple    *merged;
	int64				merged_count;
	int64			   *keys;
	FitingSegment	   *new_segs;
	int					new_num_segs;
	int					total_new_capacity;
	FitingDirEntry	   *new_entries;
	int					old_page_list_head;
	BlockNumber			old_buf_blkno;

	/* ---- Step 1: collect live tuples from data pages (walk node list) -- */
	live_data = palloc((old_seg->seg_total_tuples + num_buf)
					   * sizeof(FitingLeafTuple));

	{
		int		node_idx       = old_seg->page_list_head;
		int		page_local_num = 0;

		while (node_idx >= 0)
		{
			Buffer			buf;
			Page			page;
			FitingLeafTuple *page_tuples;
			int				n_on_page;
			int				start_slot;
			int				s;

			buf        = ReadBuffer(index, dir->pool[node_idx].page_no);
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

			node_idx = dir->pool[node_idx].next;
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
		memcpy(buf_data, FitingPageGetLeaf(page),
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

	/* ---- Step 4: FREE OLD PAGES FIRST -------------------------------- *
	 *                                                                      *
	 * Free before allocating new pages so freed blocks re-enter the        *
	 * freelist and can be reused immediately without file extension.        *
	 * fiting_free_page() uses ref-counts: shared pages (count>1) are       *
	 * only decremented, not wiped, protecting the neighbour segment's data. */

	if (merged_count == 0)
	{
		/*
		 * Entire segment dead + buffer empty.  Remove segment from directory
		 * and free all its pages.
		 */
		int node_idx = old_page_list_head;

		while (node_idx >= 0)
		{
			int next = dir->pool[node_idx].next;

			fiting_free_page(index, meta, counts, dir->pool[node_idx].page_no);
			fiting_dir_free_node(dir, node_idx);
			node_idx = next;
		}
		if (old_buf_blkno != InvalidBlockNumber)
			fiting_free_page(index, meta, counts, old_buf_blkno);

		meta->num_segments--;
		dir->hdr.num_segments--;

		memmove(&dir->entries[seg_idx],
				&dir->entries[seg_idx + 1],
				(meta->num_segments - seg_idx) * sizeof(FitingDirEntry));
		memset(&dir->entries[meta->num_segments], 0, sizeof(FitingDirEntry));

		pfree(merged);
		goto write_dir_meta;
	}

	/* Free old data pages */
	{
		int node_idx = old_page_list_head;

		while (node_idx >= 0)
		{
			int next = dir->pool[node_idx].next;

			fiting_free_page(index, meta, counts, dir->pool[node_idx].page_no);
			fiting_dir_free_node(dir, node_idx);
			node_idx = next;
		}
	}

	/* Free old buffer page */
	if (old_buf_blkno != InvalidBlockNumber)
		fiting_free_page(index, meta, counts, old_buf_blkno);

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

		/* ---- Step 5: WRITE NEW SEGMENT PAGES (freelist-first) ---------- */
		new_entries = palloc0(nfinals * sizeof(FitingDirEntry));

		for (f = 0; f < nfinals; f++)
		{
			FinalEntry *fe        = &finals[f];
			int64		seg_start = new_segs[fe->from_seg].base_rank;
			int64		seg_size  = fe->total_tuples;

			new_entries[f].start_key         = new_segs[fe->from_seg].start_key;
			new_entries[f].slope             = (fe->seg_type == FITING_SEG_TYPE_FITING)
											   ? new_segs[fe->from_seg].slope : 0.0;
			new_entries[f].seg_total_tuples  = (int32) seg_size;
			new_entries[f].seg_info          = 0;
			FitingSegSetType(&new_entries[f], fe->seg_type);
			new_entries[f].buf_blkno         = InvalidBlockNumber;
			new_entries[f].num_buffer_tuples = 0;

			fiting_write_seg_pages(index, meta, counts, dir,
								   &new_entries[f],
								   merged + seg_start, seg_size);
		}

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
