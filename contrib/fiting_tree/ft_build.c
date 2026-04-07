/*-------------------------------------------------------------------------
 *
 * ft_build.c
 *   FITing-Tree index build (ambuild / ambuildempty).
 *
 * Build algorithm (Checkpoint 1):
 *   1. Heap scan → accumulate all (key int4, TID) pairs in a palloc'd array.
 *   2. Sort the array by key (qsort).
 *   3. Run ShrinkingCone to produce linear segments.
 *   4. Write pages to the index file in order:
 *        block 0  — meta page  (FitingMetaPageData)
 *        block 1  — directory  (FitingDirEntry array, one page)
 *        block 2+ — leaf data  (FitingLeafTuple dense arrays)
 *
 * Assumptions (Checkpoint 1):
 *   - Table must be CLUSTERed on the indexed column before CREATE INDEX so
 *     that heap TIDs are in key order.  The index build itself sorts the
 *     collected (key, TID) pairs, so the sort order of the index structure
 *     is always correct even if the heap is not perfectly clustered.
 *   - int4 column only.
 *   - At most FITING_DIR_ENTRIES_MAX (340) segments fit in one directory
 *     page.  For very large or highly non-linear datasets increase
 *     max_error or extend to a multi-page directory (future work).
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
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* -----------------------------------------------------------------------
 * Build state accumulated during the heap scan
 * ----------------------------------------------------------------------- */
typedef struct FitingBuildState
{
	int64			ntups;			/* tuples collected so far */
	int64			alloc_size;		/* current palloc capacity */
	FitingLeafTuple *tuples;		/* growing array of (key, tid) pairs */
	MemoryContext	tmpCtx;
	int32			max_error;
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

	/* Skip dead tuples and NULLs — FITing-Tree requires non-null int4 keys */
	if (!tupleIsAlive || isnull[0])
		return;

	oldCtx = MemoryContextSwitchTo(bs->tmpCtx);

	/* Grow the array if needed */
	if (bs->ntups >= bs->alloc_size)
	{
		bs->alloc_size *= 2;
		bs->tuples = repalloc(bs->tuples,
							  bs->alloc_size * sizeof(FitingLeafTuple));
	}

	bs->tuples[bs->ntups].key = DatumGetInt32(values[0]);
	bs->tuples[bs->ntups].tid = *tid;
	bs->ntups++;

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(bs->tmpCtx);
}

/* -----------------------------------------------------------------------
 * write_page_to_index
 *
 * Allocate a new page in the index file, initialise it, fill it with
 * `data` bytes starting at PageGetContents(), and flush via GenericXLog.
 * Pages are allocated in order so callers rely on the implicit sequence.
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
	/* Advance pd_lower past the written data */
	((PageHeader) page)->pd_lower += data_bytes;

	GenericXLogFinish(xstate);
	UnlockReleaseBuffer(buf);
}

/* -----------------------------------------------------------------------
 * re_write_meta_page
 *
 * Update the meta page (block 0) that was already allocated.  Called after
 * all other pages have been written so that total_tuples and num_segments
 * are final.
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
	int32			   *keys;
	FitingSegment	   *segs;
	int					num_segs;
	FitingMetaPageData	meta;
	FitingDirEntry	   *dir_entries;

	/* Sanity: fresh index must be empty */
	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "fiting_tree index \"%s\" already contains data",
			 RelationGetRelationName(index));

	/* Initialise build state */
	memset(&bs, 0, sizeof(bs));
	bs.max_error = FITING_DEFAULT_MAX_ERROR;
	bs.alloc_size = 4096;
	bs.tuples = palloc(bs.alloc_size * sizeof(FitingLeafTuple));
	bs.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "fiting_tree build temporary context",
									  ALLOCSET_DEFAULT_SIZES);

	/* ---- Phase 1: heap scan ------------------------------------------ */
	reltuples = table_index_build_scan(heap, index, indexInfo,
									   true,	/* allow sync scan */
									   true,	/* report progress */
									   fiting_build_callback,
									   &bs,
									   NULL);

	MemoryContextDelete(bs.tmpCtx);

	/* Handle empty table */
	if (bs.ntups == 0)
	{
		/* Write a placeholder meta page so the index is valid */
		memset(&meta, 0, sizeof(meta));
		meta.magic = FITING_MAGIC;
		meta.max_error = bs.max_error;
		meta.total_tuples = 0;
		meta.num_segments = 0;
		meta.dir_blkno = InvalidBlockNumber;

		write_page_to_index(index, FITING_F_META, &meta, sizeof(meta));

		result = palloc(sizeof(IndexBuildResult));
		result->heap_tuples = reltuples;
		result->index_tuples = 0;
		pfree(bs.tuples);
		return result;
	}

	/* ---- Phase 2: sort by key ---------------------------------------- */
	qsort(bs.tuples, (size_t) bs.ntups, sizeof(FitingLeafTuple), leaf_tuple_cmp);

	/* ---- Phase 3: ShrinkingCone -------------------------------------- */
	keys = palloc(bs.ntups * sizeof(int32));
	for (int64 i = 0; i < bs.ntups; i++)
		keys[i] = bs.tuples[i].key;

	segs = FitingRunShrinkingCone(keys, bs.ntups, bs.max_error, &num_segs);
	pfree(keys);

	if (num_segs > FITING_DIR_ENTRIES_MAX)
		elog(ERROR,
			 "fiting_tree: too many segments (%d) for checkpoint 1 "
			 "(max %d). Increase max_error or reduce data non-linearity.",
			 num_segs, FITING_DIR_ENTRIES_MAX);

	elog(LOG, "fiting_tree build: %lld tuples → %d segments (max_error=%d, "
		 "avg segment length=%.1f)",
		 (long long) bs.ntups, num_segs, bs.max_error,
		 (double) bs.ntups / num_segs);

	/* ---- Phase 4: write index pages ---------------------------------- */

	/*
	 * Write a temporary meta page as block 0 (we need block 0 to exist
	 * before we can call ReadBuffer(index, FITING_META_BLKNO) to update it).
	 * We will overwrite it with final values after all other pages are written.
	 */
	memset(&meta, 0, sizeof(meta));
	meta.magic = FITING_MAGIC;
	meta.max_error = bs.max_error;
	meta.total_tuples = bs.ntups;
	meta.num_segments = num_segs;
	meta.dir_blkno = FITING_DIR_BLKNO;

	write_page_to_index(index, FITING_F_META, &meta, sizeof(meta));

	/* Block 1: directory page */
	dir_entries = palloc(num_segs * sizeof(FitingDirEntry));
	for (int i = 0; i < num_segs; i++)
	{
		dir_entries[i].start_key = segs[i].start_key;
		dir_entries[i].pad = 0;
		dir_entries[i].slope = segs[i].slope;
		dir_entries[i].base_rank = segs[i].base_rank;
	}
	write_page_to_index(index, FITING_F_DIR,
						dir_entries, num_segs * sizeof(FitingDirEntry));
	pfree(dir_entries);

	/* Blocks 2+: leaf data pages */
	{
		int64	written = 0;

		while (written < bs.ntups)
		{
			int64	batch = bs.ntups - written;

			if (batch > FITING_TUPLES_PER_PAGE)
				batch = FITING_TUPLES_PER_PAGE;

			write_page_to_index(index, FITING_F_LEAF,
								bs.tuples + written,
								(size_t) batch * sizeof(FitingLeafTuple));
			written += batch;
		}
	}

	/*
	 * Re-write the meta page now that total_tuples and num_segments are
	 * confirmed.  (They were the same values we used above, so this is
	 * technically a no-op in the current design, but it keeps the pattern
	 * correct for future changes.)
	 */
	re_write_meta_page(index, &meta);

	/* ---- Cleanup ----------------------------------------------------- */
	pfree(bs.tuples);
	pfree(segs);

	result = palloc(sizeof(IndexBuildResult));
	result->heap_tuples = reltuples;
	result->index_tuples = (double) bs.ntups;
	return result;
}

/* -----------------------------------------------------------------------
 * fiting_buildempty
 *
 * Called for unlogged tables: write an empty index structure on the
 * INIT_FORKNUM so crash recovery can restore a clean state.
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
	meta.magic = FITING_MAGIC;
	meta.max_error = FITING_DEFAULT_MAX_ERROR;
	meta.total_tuples = 0;
	meta.num_segments = 0;
	meta.dir_blkno = InvalidBlockNumber;

	memcpy(PageGetContents(page), &meta, sizeof(meta));
	((PageHeader) page)->pd_lower += sizeof(meta);

	GenericXLogFinish(xstate);
	UnlockReleaseBuffer(buf);
}
