/*-------------------------------------------------------------------------
 *
 * ft_insert.c
 *   FITing-Tree aminsert — Delta Insert Strategy (paper §5.2).
 *
 * Algorithm:
 *   1. Skip NULL keys and HOT-update no-ops (indexUnchanged = true).
 *   2. Binary-search directory for the segment that owns the new key.
 *   3. Load the segment's sorted buffer page into memory.
 *   4. Binary-insert the new (key, TID) into the in-memory buffer.
 *   5a. If buffer is now full (count >= max_error):
 *         Call fiting_resegment(), which:
 *           - Merges live data + buffer
 *           - Filters out FITING_LEAF_DELETED tombstones
 *           - Runs ShrinkingCone
 *           - Writes new segment pages, frees old pages
 *           - Updates directory and meta on disk
 *   5b. Otherwise:
 *         Write the updated buffer back to its page (allocating one if
 *         buf_blkno == InvalidBlockNumber).
 *         Update directory entry and meta page on disk.
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
 * read_meta_copy
 *
 * Read meta page and return a by-value copy.  Releases buffer before
 * returning so the caller holds no lock.
 * ----------------------------------------------------------------------- */
static FitingMetaPageData
read_meta_copy(Relation index)
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
	UnlockReleaseBuffer(buf);
	return meta;
}

/* -----------------------------------------------------------------------
 * read_dir_copy
 *
 * Read directory page and return a palloc'd copy of the entry array.
 * ----------------------------------------------------------------------- */
static FitingDirEntry *
read_dir_copy(Relation index, int num_segs)
{
	Buffer			buf;
	Page			page;
	FitingDirEntry *dir;

	buf  = ReadBuffer(index, FITING_DIR_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	/*
	 * Allocate the full directory capacity, not just num_segs entries.
	 * fiting_resegment() may expand the directory in-place (when one segment
	 * splits into multiple new ones); we need headroom to avoid a buffer
	 * overflow that would corrupt adjacent palloc chunks.
	 */
	dir = palloc(FITING_DIR_ENTRIES_MAX * sizeof(FitingDirEntry));
	memcpy(dir, FitingPageGetDir(page), num_segs * sizeof(FitingDirEntry));

	UnlockReleaseBuffer(buf);
	return dir;
}

/* -----------------------------------------------------------------------
 * find_segment_index
 *
 * Binary-search: largest i where dir[i].start_key <= key.
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
 * Return the index at which (key, tid) should be inserted into buf[0..n)
 * to keep it sorted ascending by key.  On duplicate keys, new entry goes
 * after existing ones (stable insert at right end of key run).
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
 * If buf_blkno is InvalidBlockNumber, allocates a new page and updates
 * seg->buf_blkno in the caller's dir copy.
 * meta may be updated (freelist_head); caller writes it to disk.
 * ----------------------------------------------------------------------- */
static void
write_buffer_page(Relation index,
				  FitingMetaPageData *meta,
				  FitingDirEntry *seg,
				  FitingLeafTuple *buf, int num_buf)
{
	BlockNumber			blkno;
	Buffer				rbuf;
	Page				page;
	GenericXLogState   *xstate;

	if (seg->buf_blkno == InvalidBlockNumber)
	{
		blkno = fiting_alloc_page(index, meta);
		seg->buf_blkno = blkno;
	}
	else
	{
		blkno = seg->buf_blkno;
	}

	rbuf = ReadBuffer(index, blkno);
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
 * write_dir_page — overwrite directory (block 1) in place
 * ----------------------------------------------------------------------- */
static void
write_dir_page(Relation index, const FitingDirEntry *dir, int num_segs)
{
	Buffer				buf;
	Page				page;
	GenericXLogState   *xstate;

	buf  = ReadBuffer(index, FITING_DIR_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	xstate = GenericXLogStart(index);
	page   = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

	FitingInitPage(page, FITING_F_DIR);
	memcpy(PageGetContents(page), dir, num_segs * sizeof(FitingDirEntry));
	((PageHeader) page)->pd_lower += num_segs * sizeof(FitingDirEntry);

	GenericXLogFinish(xstate);
	UnlockReleaseBuffer(buf);
}

/* -----------------------------------------------------------------------
 * write_meta_page — overwrite meta (block 0) in place
 * ----------------------------------------------------------------------- */
static void
write_meta_page(Relation index, const FitingMetaPageData *meta)
{
	Buffer				buf;
	Page				page;
	GenericXLogState   *xstate;

	buf  = ReadBuffer(index, FITING_META_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	xstate = GenericXLogStart(index);
	page   = GenericXLogRegisterBuffer(xstate, buf, GENERIC_XLOG_FULL_IMAGE);

	FitingInitPage(page, FITING_F_META);
	memcpy(PageGetContents(page), meta, sizeof(FitingMetaPageData));
	((PageHeader) page)->pd_lower += sizeof(FitingMetaPageData);

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
	FitingDirEntry	   *dir;
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

	/* ---- 1. Read meta + directory ------------------------------------ */
	meta = read_meta_copy(indexRelation);

	/*
	 * Edge case: index is empty (built on an empty table).  We cannot
	 * insert into a segment that doesn't exist.  Treat this as a no-op;
	 * the user should rebuild the index after the first bulk load.
	 */
	if (meta.num_segments == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("fiting_tree: cannot insert into an empty index"),
				 errhint("Populate the table and run CREATE INDEX USING fiting.")));
	}

	dir = read_dir_copy(indexRelation, meta.num_segments);

	/* ---- 2. Find the owning segment ---------------------------------- */
	seg_idx = find_segment_index(dir, meta.num_segments, key);

	/*
	 * If the key is smaller than all segment start_keys, use segment 0.
	 * The FITing-Tree guarantees all keys map to some segment; a key below
	 * the first start_key is closest to segment 0.
	 */
	if (seg_idx < 0)
		seg_idx = 0;

	seg = &dir[seg_idx];

	/* ---- 3. Load buffer ---------------------------------------------- */
	num_buf = seg->num_buffer_tuples;
	buf = load_buffer_page(indexRelation, seg->buf_blkno, num_buf);

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
		 * Buffer is full.  Temporarily put the updated buffer back into the
		 * dir entry so fiting_resegment() can read it from there.
		 * We write it to disk first, then let resegment() free it.
		 */
		write_buffer_page(indexRelation, &meta, seg, buf, num_buf);
		seg->num_buffer_tuples = num_buf;

		/* Write dir (with updated buf_blkno/num_buffer_tuples) + meta */
		write_dir_page(indexRelation, dir, meta.num_segments);
		write_meta_page(indexRelation, &meta);

		/* Re-read the freshly written dir (resegment will read from disk) */
		pfree(dir);
		dir = read_dir_copy(indexRelation, meta.num_segments);
		meta = read_meta_copy(indexRelation);

		/*
		 * fiting_resegment merges live data + buffer, runs ShrinkingCone,
		 * writes new pages, frees old pages, and writes dir + meta to disk.
		 */
		fiting_resegment(indexRelation, &meta, dir, seg_idx);

		/*
		 * meta.total_tuples was adjusted inside fiting_resegment to subtract
		 * deleted entries.  Add 1 for the newly inserted live tuple.
		 */
		meta = read_meta_copy(indexRelation);
		meta.total_tuples++;
		write_meta_page(indexRelation, &meta);
	}
	else
	{
		/* Buffer not yet full — write back and update meta/dir */
		write_buffer_page(indexRelation, &meta, seg, buf, num_buf);
		seg->num_buffer_tuples = num_buf;
		meta.total_tuples++;

		write_dir_page(indexRelation, dir, meta.num_segments);
		write_meta_page(indexRelation, &meta);
	}

	pfree(buf);
	pfree(dir);

	return false;				/* not a unique index */
}
