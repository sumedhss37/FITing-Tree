/*-------------------------------------------------------------------------
 *
 * ft_vacuum.c
 *   FITing-Tree ambulkdelete + amvacuumcleanup.
 *
 * Delete strategy (paper §5.2, lazy tombstone approach):
 *   - ambulkdelete marks dead heap TIDs with FITING_LEAF_DELETED in data pages.
 *   - Buffer pages are compacted in-place (dead entries removed immediately).
 *   - NO re-segmentation happens here.  Dead data-page tombstones will be
 *     purged the next time the segment's buffer fills and fiting_resegment()
 *     is called (which filters tombstones before running ShrinkingCone).
 *
 * Data pages are now located by walking each segment's FitingPageListNode
 * linked list (stored in dir->pool[]) instead of using the old contiguous
 * first_data_blkno + p formula.
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
 * fiting_bulkdelete
 *
 * For every segment:
 *   a) Walk data page linked list — mark dead TIDs with FITING_LEAF_DELETED.
 *   b) Compact buffer page — remove dead entries and write back (or free
 *      the page if it becomes empty).
 *
 * NO re-segmentation.  Dead data-page tombstones are cleaned by the next
 * fiting_resegment() that fires when a buffer overflows.
 * ----------------------------------------------------------------------- */
IndexBulkDeleteResult *
fiting_bulkdelete(IndexVacuumInfo *info,
				  IndexBulkDeleteResult *stats,
				  IndexBulkDeleteCallback callback,
				  void *callback_state)
{
	Relation			index = info->index;
	FitingMetaPageData	meta;
	int32			   *counts;
	FitingDirPageContent *dir;
	int					i;

	if (stats == NULL)
		stats = palloc0(sizeof(IndexBulkDeleteResult));

	counts = palloc0(FITING_META_MAX_PAGES * sizeof(int32));
	meta   = fiting_read_meta_and_counts(index, counts);

	if (meta.num_segments == 0)
	{
		pfree(counts);
		return stats;
	}

	dir = fiting_read_dir_copy(index);

	for (i = 0; i < meta.num_segments; i++)
	{
		FitingDirEntry *seg       = &dir->entries[i];
		int				node_idx  = seg->page_list_head;
		int				page_local_num = 0;

		/* ---- a) Mark dead entries in data pages (walk linked list) ------- */
		while (node_idx >= 0)
		{
			BlockNumber			blkno = dir->pool[node_idx].page_no;
			Buffer				dbuf;
			Page				dpage;
			GenericXLogState   *xstate;
			FitingLeafTuple    *tuples;
			int					tpp = FITING_TUPLES_PER_PAGE;
			int					start_slot, end_slot;
			int					s;
			bool				page_dirty = false;

			dbuf   = ReadBuffer(index, blkno);
			LockBuffer(dbuf, BUFFER_LOCK_SHARE);
			dpage  = BufferGetPage(dbuf);
			tuples = FitingPageGetLeaf(dpage);

			/*
			 * First page of segment may share slots with the previous segment.
			 * Only touch slots owned by this segment.
			 */
			start_slot = (page_local_num == 0) ? seg->start_slot : 0;

			/*
			 * Compute the last slot this segment owns on this page.
			 * absolute range of this segment: [start_slot, start_slot + seg_total_tuples)
			 * This page covers absolute indices [page_local_num*tpp, (page_local_num+1)*tpp).
			 */
			{
				int abs_seg_end  = seg->start_slot + seg->seg_total_tuples;
				int abs_page_end = (page_local_num + 1) * tpp;
				int abs_end      = (abs_seg_end < abs_page_end)
									? abs_seg_end : abs_page_end;

				end_slot = abs_end - page_local_num * tpp;
				if (end_slot > tpp)
					end_slot = tpp;
			}

			/* First pass: check whether any deletion is needed */
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

			if (page_dirty)
			{
				/* Second pass: write back under GenericXLog */
				dbuf   = ReadBuffer(index, blkno);
				LockBuffer(dbuf, BUFFER_LOCK_EXCLUSIVE);
				xstate = GenericXLogStart(index);
				dpage  = GenericXLogRegisterBuffer(xstate, dbuf,
												   0 /* keep contents */);
				tuples = FitingPageGetLeaf(dpage);

				for (s = start_slot; s < end_slot; s++)
				{
					if (!(tuples[s].flags & FITING_LEAF_DELETED) &&
						callback(&tuples[s].tid, callback_state))
					{
						tuples[s].flags |= FITING_LEAF_DELETED;
						FitingSegAddDeleted(seg, 1);
						stats->tuples_removed++;
						meta.total_tuples--;
					}
				}

				GenericXLogFinish(xstate);
				UnlockReleaseBuffer(dbuf);
			}

			node_idx = dir->pool[node_idx].next;
			page_local_num++;
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
				/* Buffer empty — free the page (count-aware) */
				fiting_free_page(index, &meta, counts, seg->buf_blkno);
				seg->buf_blkno          = InvalidBlockNumber;
				seg->num_buffer_tuples  = 0;
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

	/* Persist updated directory, meta, and counts */
	fiting_write_dir_page(index, dir);
	fiting_write_meta_and_counts(index, &meta, counts);

	stats->num_index_tuples = (double) meta.total_tuples;

	pfree(dir);
	pfree(counts);
	return stats;
}

/* -----------------------------------------------------------------------
 * fiting_vacuumcleanup
 *
 * Post-VACUUM statistics update only.  No structural work needed here.
 * ----------------------------------------------------------------------- */
IndexBulkDeleteResult *
fiting_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	FitingMetaPageData meta;

	if (stats == NULL)
		stats = palloc0(sizeof(IndexBulkDeleteResult));

	meta = fiting_read_meta_and_counts(info->index, NULL);
	stats->num_index_tuples = (double) meta.total_tuples;

	return stats;
}
