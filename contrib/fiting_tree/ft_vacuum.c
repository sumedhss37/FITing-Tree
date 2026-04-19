/*-------------------------------------------------------------------------
 *
 * ft_vacuum.c
 *   FITing-Tree ambulkdelete + amvacuumcleanup.
 *
 * Delete strategy (paper §5.2, lazy tombstone approach):
 *   - ambulkdelete marks dead heap TIDs with FITING_LEAF_DELETED in data pages.
 *   - Buffer pages are compacted in-place (dead entries removed immediately).
 *   - NO re-segmentation happens here.  Dead data-page entries persist as
 *     tombstones and are purged the next time that segment's buffer fills and
 *     fiting_resegment() is called (which filters tombstones before running
 *     ShrinkingCone).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/generic_xlog.h"
#include "commands/vacuum.h"
#include "fiting_tree.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/* -----------------------------------------------------------------------
 * read_meta_copy_vac / read_dir_copy_vac
 *
 * Local read helpers (mirror of ft_insert.c helpers; static scope here).
 * ----------------------------------------------------------------------- */
static FitingMetaPageData
read_meta_copy_vac(Relation index)
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
		elog(ERROR, "fiting_tree: bad magic in meta page (vacuum)");
	}

	meta = *meta_ptr;
	UnlockReleaseBuffer(buf);
	return meta;
}

static FitingDirEntry *
read_dir_copy_vac(Relation index, int num_segs)
{
	Buffer			buf;
	Page			page;
	FitingDirEntry *dir;

	buf  = ReadBuffer(index, FITING_DIR_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	dir = palloc(num_segs * sizeof(FitingDirEntry));
	memcpy(dir, FitingPageGetDir(page), num_segs * sizeof(FitingDirEntry));

	UnlockReleaseBuffer(buf);
	return dir;
}

static void
write_dir_page_vac(Relation index, const FitingDirEntry *dir, int num_segs)
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

static void
write_meta_page_vac(Relation index, const FitingMetaPageData *meta)
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
 * fiting_bulkdelete
 *
 * For every segment:
 *   a) Scan data pages — mark dead TIDs with FITING_LEAF_DELETED in-place.
 *   b) Compact buffer page — remove dead entries and write back (or free the
 *      page if it becomes empty).
 *
 * NO re-segmentation.  Dead data-page tombstones will be cleaned when the
 * segment's buffer next overflows and triggers fiting_resegment().
 * ----------------------------------------------------------------------- */
IndexBulkDeleteResult *
fiting_bulkdelete(IndexVacuumInfo *info,
				  IndexBulkDeleteResult *stats,
				  IndexBulkDeleteCallback callback,
				  void *callback_state)
{
	Relation		index = info->index;
	FitingMetaPageData meta;
	FitingDirEntry *dir;
	int				i;

	if (stats == NULL)
		stats = palloc0(sizeof(IndexBulkDeleteResult));

	meta = read_meta_copy_vac(index);

	if (meta.num_segments == 0)
		return stats;

	dir = read_dir_copy_vac(index, meta.num_segments);

	for (i = 0; i < meta.num_segments; i++)
	{
		FitingDirEntry *seg = &dir[i];
		int				p;

		/* ---- a) Mark dead entries in data pages ------------------------- */
		for (p = 0; p < seg->num_data_pages; p++)
		{
			Buffer				dbuf;
			Page				dpage;
			GenericXLogState   *xstate;
			FitingLeafTuple    *tuples;
			int					tpp;
			int					s;
			int					start_slot;
			int					end_slot;
			bool				page_dirty = false;

			dbuf   = ReadBuffer(index, seg->first_data_blkno + p);
			LockBuffer(dbuf, BUFFER_LOCK_SHARE);
			dpage  = BufferGetPage(dbuf);
			tuples = FitingPageGetLeaf(dpage);
			tpp    = FITING_TUPLES_PER_PAGE;

			/*
			 * The first page may share slots with the previous segment.
			 * Only touch slots [start_slot, end_slot).
			 */
			start_slot = (p == 0) ? seg->start_slot : 0;

			/*
			 * Compute the last slot this segment owns on page p.
			 * absolute range: [start_slot (if p==0), start_slot + seg_total_tuples)
			 *   page p covers absolute indices [p*tpp, (p+1)*tpp).
			 *   The segment's tuples on page p end at:
			 *     min((p+1)*tpp, start_slot + seg->seg_total_tuples) - p*tpp
			 * Simplified for the relative-slot view:
			 */
			{
				int abs_seg_end = seg->start_slot + seg->seg_total_tuples;
				int abs_page_end = (p + 1) * tpp;
				int abs_end = (abs_seg_end < abs_page_end) ? abs_seg_end : abs_page_end;
				end_slot = abs_end - p * tpp;
				if (end_slot > tpp)
					end_slot = tpp;
			}

			/* First pass: check whether any deletion is needed (avoid WAL write) */
			for (s = start_slot; s < end_slot; s++)
			{
				if (!(tuples[s].flags & FITING_LEAF_DELETED) &&
					callback(&tuples[s].tid, callback_state))
				{
					page_dirty = true;
					break;
				}
			}

			UnlockReleaseBuffer(dbuf);

			if (!page_dirty)
				continue;

			/* Second pass: write back under GenericXLog */
			dbuf   = ReadBuffer(index, seg->first_data_blkno + p);
			LockBuffer(dbuf, BUFFER_LOCK_EXCLUSIVE);
			xstate = GenericXLogStart(index);
			dpage  = GenericXLogRegisterBuffer(xstate, dbuf, 0 /* keep contents */);
			tuples = FitingPageGetLeaf(dpage);

			for (s = start_slot; s < end_slot; s++)
			{
				if (!(tuples[s].flags & FITING_LEAF_DELETED) &&
					callback(&tuples[s].tid, callback_state))
				{
					tuples[s].flags |= FITING_LEAF_DELETED;
					seg->seg_num_deleted++;
					stats->tuples_removed++;
					meta.total_tuples--;
				}
			}

			GenericXLogFinish(xstate);
			UnlockReleaseBuffer(dbuf);
		}

		/* ---- b) Compact buffer page ------------------------------------- */
		if (seg->buf_blkno != InvalidBlockNumber && seg->num_buffer_tuples > 0)
		{
			Buffer			bbuf;
			Page			bpage;
			FitingLeafTuple *src;
			FitingLeafTuple *compacted;
			int				 n = seg->num_buffer_tuples;
			int				 j, new_n = 0;

			/* Load buffer into memory */
			bbuf  = ReadBuffer(index, seg->buf_blkno);
			LockBuffer(bbuf, BUFFER_LOCK_SHARE);
			bpage = BufferGetPage(bbuf);
			src   = FitingPageGetLeaf(bpage);

			compacted = palloc(n * sizeof(FitingLeafTuple));
			for (j = 0; j < n; j++)
			{
				if (callback(&src[j].tid, callback_state))
				{
					stats->tuples_removed++;
					meta.total_tuples--;
				}
				else
				{
					compacted[new_n++] = src[j];
				}
			}
			UnlockReleaseBuffer(bbuf);

			if (new_n == 0)
			{
				/* Buffer is now empty — free the page */
				fiting_free_page(index, &meta, seg->buf_blkno);
				seg->buf_blkno           = InvalidBlockNumber;
				seg->num_buffer_tuples   = 0;
			}
			else if (new_n < n)
			{
				/* Write compacted buffer back */
				GenericXLogState *xstate;

				bbuf   = ReadBuffer(index, seg->buf_blkno);
				LockBuffer(bbuf, BUFFER_LOCK_EXCLUSIVE);
				xstate = GenericXLogStart(index);
				bpage  = GenericXLogRegisterBuffer(xstate, bbuf,
												   GENERIC_XLOG_FULL_IMAGE);

				FitingInitPage(bpage, FITING_F_LEAF);
				memcpy(PageGetContents(bpage), compacted,
					   new_n * sizeof(FitingLeafTuple));
				((PageHeader) bpage)->pd_lower += new_n * sizeof(FitingLeafTuple);

				GenericXLogFinish(xstate);
				UnlockReleaseBuffer(bbuf);

				seg->num_buffer_tuples = new_n;
			}

			pfree(compacted);
		}
	}

	/* Persist updated directory and meta */
	write_dir_page_vac(index, dir, meta.num_segments);
	write_meta_page_vac(index, &meta);

	stats->num_index_tuples = (double) meta.total_tuples;

	pfree(dir);
	return stats;
}

/* -----------------------------------------------------------------------
 * fiting_vacuumcleanup
 *
 * Post-VACUUM statistics update.  No structural work needed here since
 * ambulkdelete already did the tombstone marking and buffer compaction.
 * ----------------------------------------------------------------------- */
IndexBulkDeleteResult *
fiting_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	FitingMetaPageData meta;

	if (stats == NULL)
		stats = palloc0(sizeof(IndexBulkDeleteResult));

	meta = read_meta_copy_vac(info->index);
	stats->num_index_tuples = (double) meta.total_tuples;

	return stats;
}
