/*-------------------------------------------------------------------------
 *
 * ft_build.c
 *   FITing-Tree index build (ambuild / ambuildempty) and shared page
 *   management helpers used by ft_insert.c and ft_vacuum.c.
 *
 * Build algorithm (Checkpoint 2):
 *   1. Heap scan → accumulate all (key int4, TID) pairs.
 *   2. Sort by key (qsort).
 *   3. Run ShrinkingCone → linear segments.
 *   4. Write pages in the same packed byte layout as CP1:
 *        block 0  — meta page  (FitingMetaPageData)
 *        block 1  — directory  (FitingDirEntry array, one page)
 *        block 2+ — leaf data  (FitingLeafTuple dense arrays, packed globally)
 *
 *   The key change from CP1 is how FitingDirEntry is populated:
 *   instead of storing base_rank, we store per-segment page anchors
 *   (first_data_blkno + start_slot) computed from base_rank, then discard
 *   base_rank.  The on-disk leaf bytes are identical to CP1.
 *
 * Shared helpers also implemented here:
 *   fiting_alloc_page  — allocate a page (from free list or extend)
 *   fiting_free_page   — return a page to the free list
 *   fiting_resegment   — re-segment one segment (flush buffer + purge dead)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>				/* qsort */

#include "access/genam.h"
#include "access/generic_xlog.h"
#include "access/tableam.h"
#include "catalog/storage.h"	/* RelationTruncate */
#include "fiting_tree.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
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
	Oid				keytype;	/* atttypid of the indexed column */
} FitingBuildState;

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
 * Allocate a new page via ExtendBufferedRel, initialise it, fill with
 * data, and flush via GenericXLog.  Pages are allocated in order.
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
	page = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

	FitingInitPage(page, page_flags);
	memcpy(PageGetContents(page), data, data_bytes);
	((PageHeader) page)->pd_lower += data_bytes;

	GenericXLogFinish(xstate);
	UnlockReleaseBuffer(buf);
}

/* -----------------------------------------------------------------------
 * overwrite_or_extend_page
 *
 * Write page_flags + data_bytes of data to block blkno.
 * If the block already exists, overwrite it (ReadBuffer + GenericXLog).
 * If not, extend the relation (ExtendBufferedRel).
 * ----------------------------------------------------------------------- */
static void
overwrite_or_extend_page(Relation index, BlockNumber blkno,
						 uint16 page_flags,
						 const void *data, size_t data_bytes)
{
	Buffer				buf;
	Page				page;
	GenericXLogState   *xstate;

	if (blkno < RelationGetNumberOfBlocks(index))
	{
		buf = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	}
	else
	{
		buf = ExtendBufferedRel(BMR_REL(index), MAIN_FORKNUM, NULL,
								EB_LOCK_FIRST);
		Assert(BufferGetBlockNumber(buf) == blkno);
	}

	xstate = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

	FitingInitPage(page, page_flags);
	if (data_bytes > 0)
	{
		memcpy(PageGetContents(page), data, data_bytes);
		((PageHeader) page)->pd_lower += data_bytes;
	}

	GenericXLogFinish(xstate);
	UnlockReleaseBuffer(buf);
}

/* -----------------------------------------------------------------------
 * re_write_meta_page — update block 0 in place
 * ----------------------------------------------------------------------- */
static void
re_write_meta_page(Relation index, FitingMetaPageData *meta)
{
	Buffer				buf;
	Page				page;
	GenericXLogState   *xstate;

	buf = ReadBuffer(index, FITING_META_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	xstate = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

	FitingInitPage(page, FITING_F_META);
	memcpy(PageGetContents(page), meta, sizeof(FitingMetaPageData));
	((PageHeader) page)->pd_lower += sizeof(FitingMetaPageData);

	GenericXLogFinish(xstate);
	UnlockReleaseBuffer(buf);
}

/* -----------------------------------------------------------------------
 * fiting_build
 * ----------------------------------------------------------------------- */
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
	FitingDirEntry	   *dir_entries;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "fiting_tree index \"%s\" already contains data",
			 RelationGetRelationName(index));

	/* Initialise build state */
	memset(&bs, 0, sizeof(bs));
	bs.max_error  = FITING_DEFAULT_MAX_ERROR;
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

	/* Handle empty table */
	if (bs.ntups == 0)
	{
		memset(&meta, 0, sizeof(meta));
		meta.magic        = FITING_MAGIC;
		meta.max_error    = bs.max_error;
		meta.total_tuples = 0;
		meta.num_segments = 0;
		meta.dir_blkno    = InvalidBlockNumber;
		meta.freelist_head = InvalidBlockNumber;

		write_page_to_index(index, FITING_F_META, &meta, sizeof(meta));

		result = palloc(sizeof(IndexBuildResult));
		result->heap_tuples  = reltuples;
		result->index_tuples = 0;
		pfree(bs.tuples);
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

	if (num_segs > FITING_DIR_ENTRIES_MAX)
		elog(ERROR,
			 "fiting_tree: too many segments (%d, max %d). "
			 "Increase max_error or reduce data non-linearity.",
			 num_segs, FITING_DIR_ENTRIES_MAX);

	elog(LOG, "fiting_tree build: %lld tuples → %d segments "
		 "(max_error=%d, avg seg len=%.1f)",
		 (long long) bs.ntups, num_segs, bs.max_error,
		 (double) bs.ntups / num_segs);

	/* ---- Phase 4: build directory entries ----------------------------- */
	/*
	 * Leaf data is written in the same packed global order as CP1, so
	 * segments share pages at boundaries.  start_slot captures the
	 * intra-page offset of each segment's first tuple.
	 * first_data_blkno is computed arithmetically (pages are contiguous
	 * starting at block 2, matching the ExtendBufferedRel write order above).
	 */
	dir_entries = palloc0(num_segs * sizeof(FitingDirEntry));

	for (int i = 0; i < num_segs; i++)
	{
		int64		base     = segs[i].base_rank;
		int64		seg_end  = (i + 1 < num_segs) ? segs[i + 1].base_rank
												  : bs.ntups;
		int64		sz       = seg_end - base;
		BlockNumber first_blk;
		int			n_pages;

		dir_entries[i].start_key          = segs[i].start_key;
		dir_entries[i].slope              = segs[i].slope;
		dir_entries[i].start_slot         = (int32) (base % FITING_TUPLES_PER_PAGE);
		dir_entries[i].seg_total_tuples   = (int32) sz;
		dir_entries[i].seg_num_deleted    = 0;
		dir_entries[i].buf_blkno          = InvalidBlockNumber;
		dir_entries[i].num_buffer_tuples  = 0;

		first_blk = FITING_DATA_START_BLKNO
					+ (BlockNumber) (base / FITING_TUPLES_PER_PAGE);

		n_pages = (dir_entries[i].start_slot + (int) sz
				   + FITING_TUPLES_PER_PAGE - 1)
				  / FITING_TUPLES_PER_PAGE;

		dir_entries[i].first_data_blkno = first_blk;
		dir_entries[i].num_data_pages   = n_pages;
	}

	/* ---- Phase 5: write index pages ----------------------------------- */
	/* Placeholder meta (block 0) */
	memset(&meta, 0, sizeof(meta));
	meta.magic         = FITING_MAGIC;
	meta.max_error     = bs.max_error;
	meta.total_tuples  = bs.ntups;
	meta.num_segments  = num_segs;
	meta.dir_blkno     = FITING_DIR_BLKNO;
	meta.freelist_head = InvalidBlockNumber;

	write_page_to_index(index, FITING_F_META, &meta, sizeof(meta));

	/* Block 1: directory */
	write_page_to_index(index, FITING_F_DIR,
						dir_entries, num_segs * sizeof(FitingDirEntry));
	pfree(dir_entries);

	/* Blocks 2+: leaf data — identical byte layout to CP1 */
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

	re_write_meta_page(index, &meta);

	pfree(bs.tuples);
	pfree(segs);

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
	page = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

	FitingInitPage(page, FITING_F_META);

	memset(&meta, 0, sizeof(meta));
	meta.magic         = FITING_MAGIC;
	meta.max_error     = FITING_DEFAULT_MAX_ERROR;
	meta.total_tuples  = 0;
	meta.num_segments  = 0;
	meta.dir_blkno     = InvalidBlockNumber;
	meta.freelist_head = InvalidBlockNumber;

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
 * Return a block number ready for use.  First tries the free list stored
 * in meta->freelist_head; if empty, extends the relation.
 *
 * The caller is responsible for writing meta back to disk after this call
 * (since freelist_head may have changed).
 * ----------------------------------------------------------------------- */
BlockNumber
fiting_alloc_page(Relation index, FitingMetaPageData *meta)
{
	if (meta->freelist_head != InvalidBlockNumber)
	{
		BlockNumber		blkno = meta->freelist_head;
		Buffer			buf;
		Page			page;
		BlockNumber		next;

		/* Read the free page to get the next pointer */
		buf = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		next = FitingFreePage_GetNext(page);
		UnlockReleaseBuffer(buf);

		meta->freelist_head = next;
		return blkno;
	}
	else
	{
		Buffer buf;
		BlockNumber blkno;

		buf   = ExtendBufferedRel(BMR_REL(index), MAIN_FORKNUM, NULL,
								  EB_LOCK_FIRST);
		blkno = BufferGetBlockNumber(buf);
		UnlockReleaseBuffer(buf);
		return blkno;
	}
}

/* -----------------------------------------------------------------------
 * fiting_free_page
 *
 * Mark block blkno as free and prepend it to the free list in meta.
 * The caller must write meta back to disk afterward.
 * ----------------------------------------------------------------------- */
void
fiting_free_page(Relation index, FitingMetaPageData *meta, BlockNumber blkno)
{
	Buffer				buf;
	Page				page;
	GenericXLogState   *xstate;

	buf = ReadBuffer(index, blkno);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	xstate = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

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
 * Write tuples[0..ntups) to freshly allocated pages for a new segment.
 * Fills in entry->first_data_blkno, entry->num_data_pages, entry->start_slot=0.
 * Allocates pages via fiting_alloc_page.
 * The caller must write meta back to disk after this call.
 * ----------------------------------------------------------------------- */
static void
fiting_write_seg_pages(Relation index,
					   FitingDirEntry *entry,
					   FitingLeafTuple *tuples, int64 ntups)
{
	int64		written = 0;
	int			page_num = 0;
	BlockNumber first_blkno = InvalidBlockNumber;

	entry->start_slot       = 0;
	entry->first_data_blkno = InvalidBlockNumber;
	entry->num_data_pages   = 0;

	while (written < ntups)
	{
		int64		batch = ntups - written;
		Buffer		buf;
		Page		page;
		GenericXLogState *xstate;

		if (batch > FITING_TUPLES_PER_PAGE)
			batch = FITING_TUPLES_PER_PAGE;

		/*
		 * Always extend the relation — never use the freelist for segment data
		 * pages.  This guarantees that all pages for one segment are
		 * physically contiguous, so the address formula
		 *   blkno = first_data_blkno + page_idx
		 * is valid without storing an explicit page map.
		 */
		buf = ExtendBufferedRel(BMR_REL(index), MAIN_FORKNUM, NULL,
								EB_LOCK_FIRST);

		if (page_num == 0)
			first_blkno = BufferGetBlockNumber(buf);

		xstate = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

		FitingInitPage(page, FITING_F_LEAF);
		memcpy(PageGetContents(page), tuples + written,
			   (size_t) batch * sizeof(FitingLeafTuple));
		((PageHeader) page)->pd_lower += (int) batch * sizeof(FitingLeafTuple);

		GenericXLogFinish(xstate);
		UnlockReleaseBuffer(buf);

		page_num++;
		written += batch;
	}

	entry->first_data_blkno = first_blkno;
	entry->num_data_pages   = page_num;
}

/* -----------------------------------------------------------------------
 * fiting_resegment
 *
 * Re-segment dir[seg_idx]:
 *   1. Collect live (non-deleted) tuples from data pages.
 *   2. Merge with buffer page tuples (always live).
 *   3. Run ShrinkingCone on the merged sorted array.
 *   4. Write new segment pages; free old ones.
 *   5. Splice new FitingDirEntry records into dir[]; update meta.
 *   6. Write directory and meta pages.
 *
 * dir[] must be a writable palloc'd copy of the full directory.
 * meta must be a writable copy of the meta page.
 * Both are updated in-memory AND written to disk before returning.
 * ----------------------------------------------------------------------- */
void
fiting_resegment(Relation index, FitingMetaPageData *meta,
				 FitingDirEntry *dir, int seg_idx)
{
	FitingDirEntry	   *old_seg = &dir[seg_idx];
	FitingLeafTuple    *live_data;
	int64				live_count = 0;
	FitingLeafTuple    *buf_data  = NULL;
	int32				num_buf    = old_seg->num_buffer_tuples;
	FitingLeafTuple    *merged;
	int64				merged_count;
	int64			   *keys;
	FitingSegment	   *new_segs;
	int					new_num_segs;
	int					total_new_capacity;
	FitingDirEntry	   *new_entries;

	/* ---- Step 1: collect live tuples from data pages ----------------- */
	live_data = palloc((old_seg->seg_total_tuples + num_buf)
					   * sizeof(FitingLeafTuple));

	for (int p = 0; p < old_seg->num_data_pages; p++)
	{
		Buffer	buf;
		Page	page;
		FitingLeafTuple *page_tuples;
		int		n_on_page;

		buf = ReadBuffer(index, old_seg->first_data_blkno + p);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		page_tuples = FitingPageGetLeaf(page);
		n_on_page = (int) ((((PageHeader) page)->pd_lower
							- SizeOfPageHeaderData)
						   / sizeof(FitingLeafTuple));

		for (int s = 0; s < n_on_page; s++)
		{
			/*
			 * For the first page, only slots >= start_slot belong to this
			 * segment; earlier slots belong to the previous segment.
			 */
			if (p == 0 && s < old_seg->start_slot)
				continue;

			/*
			 * For non-first pages, all slots are this segment's data,
			 * but we must not read past seg_total_tuples entries total.
			 */
			if (live_count + old_seg->seg_num_deleted
				>= old_seg->seg_total_tuples)
				break;

			if (!(page_tuples[s].flags & FITING_LEAF_DELETED))
				live_data[live_count++] = page_tuples[s];
		}

		UnlockReleaseBuffer(buf);
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
	merged = palloc(merged_count * sizeof(FitingLeafTuple));

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

	/* ---- Step 4: ShrinkingCone on merged array ----------------------- */
	if (merged_count == 0)
	{
		/*
		 * Entire segment is dead + buffer was empty.  Remove segment from
		 * directory and free its pages.
		 */
		for (int p = 0; p < old_seg->num_data_pages; p++)
			fiting_free_page(index, meta, old_seg->first_data_blkno + p);
		if (old_seg->buf_blkno != InvalidBlockNumber)
			fiting_free_page(index, meta, old_seg->buf_blkno);

		meta->num_segments--;

		memmove(&dir[seg_idx], &dir[seg_idx + 1],
				(meta->num_segments - seg_idx) * sizeof(FitingDirEntry));
		memset(&dir[meta->num_segments], 0, sizeof(FitingDirEntry));

		pfree(merged);
		goto write_dir_meta;
	}

	keys = palloc(merged_count * sizeof(int64));
	for (int64 i = 0; i < merged_count; i++)
		keys[i] = merged[i].key;

	new_segs = FitingRunShrinkingCone(keys, merged_count,
									  meta->max_error, &new_num_segs);
	pfree(keys);

	/* Validate we still fit in one directory page */
	total_new_capacity = meta->num_segments - 1 + new_num_segs;
	if (total_new_capacity > FITING_DIR_ENTRIES_MAX)
		elog(ERROR,
			 "fiting_tree: re-segmentation would produce %d total segments "
			 "(max %d). Increase max_error.",
			 total_new_capacity, FITING_DIR_ENTRIES_MAX);

	/* ---- Step 5: write new segment pages ----------------------------- */
	new_entries = palloc0(new_num_segs * sizeof(FitingDirEntry));

	for (int ns = 0; ns < new_num_segs; ns++)
	{
		int64		seg_start = new_segs[ns].base_rank;
		int64		seg_end   = (ns + 1 < new_num_segs)
								? new_segs[ns + 1].base_rank
								: merged_count;
		int64		seg_size  = seg_end - seg_start;

		new_entries[ns].start_key           = new_segs[ns].start_key;
		new_entries[ns].slope               = new_segs[ns].slope;
		new_entries[ns].seg_total_tuples    = (int32) seg_size;
		new_entries[ns].seg_num_deleted     = 0;
		new_entries[ns].buf_blkno           = InvalidBlockNumber;
		new_entries[ns].num_buffer_tuples   = 0;

		fiting_write_seg_pages(index,
							   &new_entries[ns],
							   merged + seg_start, seg_size);
	}

	pfree(merged);
	pfree(new_segs);

	/* ---- Step 6: free old pages -------------------------------------- */
	for (int p = 0; p < old_seg->num_data_pages; p++)
		fiting_free_page(index, meta, old_seg->first_data_blkno + p);
	if (old_seg->buf_blkno != InvalidBlockNumber)
		fiting_free_page(index, meta, old_seg->buf_blkno);

	/* Update live tuple count: old total - old deleted + newly inserted */
	meta->total_tuples -= old_seg->seg_num_deleted;

	/* ---- Step 7: splice new entries into dir[] ----------------------- */
	if (new_num_segs == 1)
	{
		dir[seg_idx] = new_entries[0];
	}
	else
	{
		/* Make room for extra entries */
		int tail = meta->num_segments - seg_idx - 1;

		memmove(&dir[seg_idx + new_num_segs],
				&dir[seg_idx + 1],
				tail * sizeof(FitingDirEntry));

		for (int ns = 0; ns < new_num_segs; ns++)
			dir[seg_idx + ns] = new_entries[ns];

		meta->num_segments += new_num_segs - 1;
	}

	pfree(new_entries);

write_dir_meta:
	/* ---- Step 8: write directory and meta to disk -------------------- */
	overwrite_or_extend_page(index, FITING_DIR_BLKNO, FITING_F_DIR,
							 dir,
							 meta->num_segments * sizeof(FitingDirEntry));
	re_write_meta_page(index, meta);
}
