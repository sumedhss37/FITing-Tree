/*-------------------------------------------------------------------------
 *
 * ft_insert.c
 *   FITing-Tree aminsert — Delta Insert Strategy (paper §5.2).
 *
 * Algorithm:
 *   1. Skip NULL keys and HOT-update no-ops (indexUnchanged = true).
 *   2. Read meta + page_tuple_counts and the full directory page.
 *   3. Binary-search directory entries for the segment that owns the key.
 *   4. Load the segment's sorted buffer page into memory.
 *   5. Binary-insert the new (key, TID) into the in-memory buffer.
 *   5a. If buffer is now full (count >= max_error):
 *         Write the updated buffer to disk so fiting_resegment can read it.
 *         Call fiting_resegment(), which:
 *           - Collects live data + buffer (walks node list)
 *           - Frees old pages (ref-count aware)
 *           - Allocates new pages (freelist-first)
 *           - Updates directory and meta on disk
 *   5b. Otherwise:
 *         Write the updated buffer back to its page (allocating one if
 *         buf_blkno == InvalidBlockNumber).
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
 * load_buffer_page
 *
 * Load up to num_buf entries from buf_blkno into a palloc'd array.
 * Returns an empty (but non-NULL) array when buf_blkno is invalid.
 * ----------------------------------------------------------------------- */
static FitingLeafTuple *
load_buffer_page(Relation index, BlockNumber buf_blkno, int num_buf)
{
	FitingLeafTuple *buf;

	buf = palloc((num_buf > 0 ? num_buf : 1) * sizeof(FitingLeafTuple));

	if (buf_blkno != InvalidBlockNumber && num_buf > 0)
	{
		Buffer	rbuf;
		Page	page;

		rbuf = ReadBuffer(index, buf_blkno);
		LockBuffer(rbuf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(rbuf);
		memcpy(buf, FitingPageGetLeaf(page),
			   num_buf * sizeof(FitingLeafTuple));
		UnlockReleaseBuffer(rbuf);
	}

	return buf;
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
 * write_buffer_page
 *
 * Write buf[0..num_buf) to the segment's buffer page.
 * If buf_blkno is InvalidBlockNumber, allocates a new page via
 * fiting_alloc_page (which sets counts[blkno]=1) and updates seg->buf_blkno.
 * meta and counts may be modified (freelist_head, counts[]); caller writes
 * them back to disk.
 * ----------------------------------------------------------------------- */
static void
write_buffer_page(Relation index,
				  FitingMetaPageData *meta,
				  int32 *counts,
				  FitingDirEntry *seg,
				  FitingLeafTuple *buf, int num_buf)
{
	BlockNumber			blkno;
	Buffer				rbuf;
	Page				page;
	GenericXLogState   *xstate;

	if (seg->buf_blkno == InvalidBlockNumber)
	{
		blkno          = fiting_alloc_page(index, meta, counts);
		seg->buf_blkno = blkno;
	}
	else
	{
		blkno = seg->buf_blkno;
	}

	rbuf   = ReadBuffer(index, blkno);
	LockBuffer(rbuf, BUFFER_LOCK_EXCLUSIVE);
	xstate = GenericXLogStart(index);
	page   = GenericXLogRegisterBuffer(xstate, rbuf, GENERIC_XLOG_FULL_IMAGE);

	FitingInitPage(page, FITING_F_LEAF);
	memcpy(PageGetContents(page), buf, num_buf * sizeof(FitingLeafTuple));
	((PageHeader) page)->pd_lower += num_buf * sizeof(FitingLeafTuple);

	GenericXLogFinish(xstate);
	UnlockReleaseBuffer(rbuf);
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
	FitingLeafTuple    *buf;
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

	/* ---- 3. Load buffer ---------------------------------------------- */
	num_buf = seg->num_buffer_tuples;
	buf     = load_buffer_page(indexRelation, seg->buf_blkno, num_buf);

	/* ---- 4. Binary-insert into in-memory buffer ---------------------- */
	buf = repalloc(buf, (num_buf + 1) * sizeof(FitingLeafTuple));

	ins_pos = binary_find_insert_pos(buf, num_buf, key);

	if (ins_pos < num_buf)
		memmove(&buf[ins_pos + 1], &buf[ins_pos],
				(num_buf - ins_pos) * sizeof(FitingLeafTuple));

	buf[ins_pos].key   = key;
	buf[ins_pos].tid   = *heap_tid;
	buf[ins_pos].flags = 0;
	num_buf++;

	/* ---- 5. Flush or write-back -------------------------------------- */
	if (num_buf >= meta.max_error)
	{
		/*
		 * Buffer is full.  Write the updated buffer to disk so that
		 * fiting_resegment() can read it, then call resegment.
		 */
		write_buffer_page(indexRelation, &meta, counts, seg, buf, num_buf);
		seg->num_buffer_tuples = num_buf;

		/* Persist dir + meta+counts before resegment reads them */
		fiting_write_dir_page(indexRelation, dir);
		fiting_write_meta_and_counts(indexRelation, &meta, counts);

		/* Re-read fresh copies (resegment reads from disk) */
		pfree(dir);
		dir  = fiting_read_dir_copy(indexRelation);
		meta = fiting_read_meta_and_counts(indexRelation, counts);

		/*
		 * fiting_resegment merges live data + buffer, runs ShrinkingCone,
		 * writes new pages, frees old pages, updates dir + meta+counts on disk.
		 * On return, meta and dir in memory reflect the post-resegment state.
		 */
		fiting_resegment(indexRelation, &meta, counts, dir, seg_idx);

		/*
		 * Re-read meta to get the post-resegment state (resegment wrote it),
		 * increment total for the newly inserted live tuple.
		 */
		meta = fiting_read_meta_and_counts(indexRelation, counts);
		meta.total_tuples++;
		fiting_write_meta_and_counts(indexRelation, &meta, counts);
	}
	else
	{
		/* Buffer not yet full — write back and update meta/dir */
		write_buffer_page(indexRelation, &meta, counts, seg, buf, num_buf);
		seg->num_buffer_tuples = num_buf;
		meta.total_tuples++;

		fiting_write_dir_page(indexRelation, dir);
		fiting_write_meta_and_counts(indexRelation, &meta, counts);
	}

	pfree(buf);
	pfree(dir);
	pfree(counts);

	return false;				/* not a unique index */
}
