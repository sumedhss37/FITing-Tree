/*-------------------------------------------------------------------------
 *
 * ft_insert.c
 *   FITing-Tree aminsert — Delta Insert Strategy (paper §5.2).
 *
 * Algorithm:
 *   1. Skip NULL keys and HOT-update no-ops (indexUnchanged = true).
 *   2. Read meta + page_tuple_counts and the full directory page.
 *   3. Binary-search directory entries for the segment that owns the key.
 *   4. Ensure the segment has a buffer slot allocated on a shared buffer
 *      page (fiting_alloc_buf_slot).  Each slot holds up to
 *      FITING_BUF_TUPLES_PER_SEG (32) tuples.
 *   5. Load the segment's slot from the shared buffer page into a local
 *      array, binary-insert the new (key, TID), and write it back.
 *   5a. If the slot is now full (count >= FITING_BUF_TUPLES_PER_SEG):
 *         Write the updated slot to disk.
 *         Call fiting_resegment(), which:
 *           - Collects live data + buffer slot (slot-offset aware)
 *           - Reuses old pages in-place, appends to data_curr_page,
 *             then allocates from freelist as needed
 *           - Updates directory and meta on disk
 *   5b. Otherwise:
 *         Write the updated slot back to its position on the shared page.
 *         Write directory and meta+counts to disk.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/generic_xlog.h"
#include "fiting_tree.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/* -----------------------------------------------------------------------
 * find_segment_index
 *
 * Binary-search: largest i where entries[i].start_key <= key.
 * Returns -1 if key < entries[0].start_key.
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
 * binary_find_insert_pos
 *
 * Return the index at which key should be inserted into buf[0..n) to
 * keep it sorted ascending.  On duplicate keys, new entry goes after
 * existing ones (stable insert at right end of key run).
 * ----------------------------------------------------------------------- */
static int
binary_find_insert_pos(const FitingLeafTuple *buf, int n, int64 key)
{
	int lo = 0, hi = n;

	while (lo < hi)
	{
		int mid = lo + (hi - lo) / 2;

		if (buf[mid].key <= key)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

/* -----------------------------------------------------------------------
 * load_buf_slot
 *
 * Copy the segment's slot from the shared buffer page into local_buf.
 * num_buf tuples are copied starting at the correct slot offset.
 * ----------------------------------------------------------------------- */
static void
load_buf_slot(Relation index, const FitingDirEntry *seg,
			  FitingLeafTuple *local_buf, int num_buf)
{
	Buffer	buf;
	Page	page;

	buf  = ReadBuffer(index, seg->buf_blkno);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	memcpy(local_buf,
		   FitingPageGetLeaf(page) +
		   FitingBufSlot(seg) * FITING_BUF_TUPLES_PER_SEG,
		   num_buf * sizeof(FitingLeafTuple));

	UnlockReleaseBuffer(buf);
}

/* -----------------------------------------------------------------------
 * write_buf_slot
 *
 * Write local_buf[0..count) back to the segment's slot on the shared
 * buffer page at the correct slot offset.  Uses GenericXLog flag 0
 * (keep existing page content) to avoid wiping other segments' slots.
 * ----------------------------------------------------------------------- */
static void
write_buf_slot(Relation index, const FitingDirEntry *seg,
			   const FitingLeafTuple *local_buf, int count)
{
	Buffer			   buf;
	Page			   page;
	GenericXLogState  *xstate;
	char			  *slot_ptr;

	buf    = ReadBuffer(index, seg->buf_blkno);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	xstate = GenericXLogStart(index);
	/* flag=0: keep existing page content, update only our slot region */
	page   = GenericXLogRegisterBuffer(xstate, buf, 0);

	slot_ptr = (char *) PageGetContents(page) +
			   FitingBufSlot(seg) * FITING_BUF_TUPLES_PER_SEG *
			   sizeof(FitingLeafTuple);
	memcpy(slot_ptr, local_buf, count * sizeof(FitingLeafTuple));

	GenericXLogFinish(xstate);
	UnlockReleaseBuffer(buf);
}

/* -----------------------------------------------------------------------
 * fiting_insert — main entry point
 * ----------------------------------------------------------------------- */
bool
fiting_insert(Relation indexRelation,
			  Datum *values,
			  bool *isnull,
			  ItemPointer heap_tid,
			  Relation heapRelation,
			  IndexUniqueCheck checkUnique,
			  bool indexUnchanged,
			  struct IndexInfo *indexInfo)
{
	int64				key;
	FitingMetaPageData	meta;
	int32			   *counts;
	FitingDirPageContent *dir;
	int					seg_idx;
	FitingDirEntry	   *seg;
	FitingLeafTuple		local_buf[FITING_BUF_TUPLES_PER_SEG];
	int					num_buf;
	int					ins_pos;

	/* Skip NULLs */
	if (isnull[0])
		return false;

	/*
	 * HOT update: the indexed column did not change, so the existing index
	 * entry remains valid.  No new entry is needed.
	 */
	if (indexUnchanged)
		return false;

	key = FitingDatumGetKey(values[0],
							TupleDescAttr(RelationGetDescr(indexRelation), 0)->atttypid);

	/* ---- 1. Read meta + counts + directory --------------------------- */
	counts = palloc0(FITING_META_MAX_PAGES * sizeof(int32));
	meta   = fiting_read_meta_and_counts(indexRelation, counts);

	/*
	 * Edge case: index is empty.  We cannot insert into a segment that
	 * doesn't exist.  The user should rebuild after the first bulk load.
	 */
	if (meta.num_segments == 0)
	{
		pfree(counts);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("fiting_tree: cannot insert into an empty index"),
				 errhint("Populate the table and run CREATE INDEX USING fiting.")));
	}

	dir = fiting_read_dir_copy(indexRelation);

	/* ---- 2. Find the owning segment ---------------------------------- */
	seg_idx = find_segment_index(dir->entries, dir->hdr.num_segments, key);

	/*
	 * If the key is smaller than all segment start_keys, use segment 0.
	 */
	if (seg_idx < 0)
		seg_idx = 0;

	seg = &dir->entries[seg_idx];

	/* ---- 3. Ensure this segment has a buffer slot -------------------- */
	if (seg->buf_blkno == InvalidBlockNumber)
	{
		/*
		 * First insert ever for this segment.  Allocate a 32-tuple slot on
		 * the current shared buffer page (or a new page if full/absent).
		 * meta + counts will be updated in memory; they get persisted below.
		 */
		fiting_alloc_buf_slot(indexRelation, &meta, counts, seg);
		num_buf = 0;
	}
	else
	{
		num_buf = FitingBufCount(seg);
		load_buf_slot(indexRelation, seg, local_buf, num_buf);
	}

	/* ---- 4. Binary-insert into local buffer -------------------------- */
	ins_pos = binary_find_insert_pos(local_buf, num_buf, key);

	if (ins_pos < num_buf)
		memmove(&local_buf[ins_pos + 1], &local_buf[ins_pos],
				(num_buf - ins_pos) * sizeof(FitingLeafTuple));

	local_buf[ins_pos].key   = key;
	local_buf[ins_pos].tid   = *heap_tid;
	local_buf[ins_pos].flags = 0;
	num_buf++;

	/* ---- 5. Flush or write-back -------------------------------------- */
	if (num_buf >= FITING_BUF_TUPLES_PER_SEG)
	{
		/*
		 * Slot is full.  Write it to disk so that fiting_resegment() can
		 * read it, then trigger resegmentation.
		 */
		FitingBufSetInfo(seg, FitingBufSlot(seg), num_buf);
		write_buf_slot(indexRelation, seg, local_buf, num_buf);

		/* Persist dir + meta+counts before resegment reads them */
		fiting_write_dir_page(indexRelation, dir);
		fiting_write_meta_and_counts(indexRelation, &meta, counts);

		/* Re-read fresh copies (resegment reads from disk) */
		pfree(dir);
		dir  = fiting_read_dir_copy(indexRelation);
		meta = fiting_read_meta_and_counts(indexRelation, counts);

		/*
		 * fiting_resegment merges live data + buffer slot, runs ShrinkingCone,
		 * writes new pages (reuse-first), updates dir + meta+counts on disk.
		 */
		fiting_resegment(indexRelation, &meta, counts, dir, seg_idx);

		/*
		 * Re-read meta to get the post-resegment state, then increment total
		 * for the newly inserted live tuple.
		 */
		meta = fiting_read_meta_and_counts(indexRelation, counts);
		meta.total_tuples++;
		fiting_write_meta_and_counts(indexRelation, &meta, counts);
	}
	else
	{
		/* Slot not yet full — write back and update meta/dir */
		FitingBufSetInfo(seg, FitingBufSlot(seg), num_buf);
		write_buf_slot(indexRelation, seg, local_buf, num_buf);
		meta.total_tuples++;

		fiting_write_dir_page(indexRelation, dir);
		fiting_write_meta_and_counts(indexRelation, &meta, counts);
	}

	pfree(dir);
	pfree(counts);

	return false;				/* not a unique index */
}
