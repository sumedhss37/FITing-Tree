/*-------------------------------------------------------------------------
 *
 * ft_info.c
 *   fiting_index_info(regclass) — set-returning function that exposes the
 *   live structure of a FITing-Tree index without reading raw pages manually.
 *
 * Returns one row per directory segment:
 *
 *   seg_idx      int4    — 0-based segment position in directory
 *   seg_type     text    — "fiting" or "btree"
 *   start_key    int8    — first key of the segment's range
 *   total_tuples int4    — total (live + deleted) tuples in data pages
 *   num_deleted  int4    — tombstone count (set by ambulkdelete)
 *   data_pages   int4    — number of linked-list data page nodes
 *   buf_tuples   int4    — tuples currently in the segment's buffer page
 *
 * Usage:
 *   -- Per-segment breakdown
 *   SELECT * FROM fiting_index_info('demo_fiting_idx');
 *
 *   -- Summary: how many segments of each type, total tuples
 *   SELECT seg_type,
 *          count(*)         AS num_segments,
 *          sum(total_tuples) AS total_tuples,
 *          sum(num_deleted)  AS deleted_tuples
 *   FROM   fiting_index_info('demo_fiting_idx')
 *   GROUP BY seg_type;
 *
 *   -- Index-level metadata (max_error, total live tuples, etc.)
 *   SELECT * FROM fiting_index_meta('demo_fiting_idx');
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relation.h"
#include "fiting_tree.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/rel.h"

/* -----------------------------------------------------------------------
 * fiting_index_info
 * ----------------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(fiting_index_info);

Datum
fiting_index_info(PG_FUNCTION_ARGS)
{
	Oid					indexoid = PG_GETARG_OID(0);
	ReturnSetInfo	   *rsinfo   = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation			index;
	FitingMetaPageData	meta;
	FitingDirPageContent *dir;
	int					i;

	/* Set up the tuplestore for materialised SRF output */
	InitMaterializedSRF(fcinfo, 0);

	/* Open the index with a shared lock */
	index = relation_open(indexoid, AccessShareLock);

	/*
	 * Quick sanity-check: read meta and bail gracefully on empty index
	 * rather than crashing on missing directory content.
	 */
	meta = fiting_read_meta_and_counts(index, NULL);

	if (meta.num_segments == 0)
	{
		relation_close(index, AccessShareLock);
		return (Datum) 0;
	}

	dir = fiting_read_dir_copy(index);

	for (i = 0; i < meta.num_segments; i++)
	{
		FitingDirEntry *seg    = &dir->entries[i];
		Datum			values[7];
		bool			nulls[7];
		int				npages   = 0;
		int				node_idx = seg->page_list_head;

		memset(nulls, 0, sizeof(nulls));

		/* Count linked-list data pages for this segment */
		while (node_idx >= 0)
		{
			FitingPageListNode nd;

			npages++;
			fiting_get_node(index, dir, node_idx, &nd);
			node_idx = nd.next;
		}

		values[0] = Int32GetDatum(i);
		values[1] = CStringGetTextDatum(
			FitingSegType(seg) == FITING_SEG_TYPE_FITING ? "fiting" : "btree");
		values[2] = Int64GetDatum(seg->start_key);
		values[3] = Int32GetDatum(seg->seg_total_tuples);
		values[4] = Int32GetDatum(FitingSegNumDeleted(seg));
		values[5] = Int32GetDatum(npages);
		values[6] = Int32GetDatum(FitingBufCount(seg));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	pfree(dir);
	relation_close(index, AccessShareLock);

	return (Datum) 0;
}

/* -----------------------------------------------------------------------
 * fiting_index_meta
 *
 * Returns a single row of index-level metadata from the meta page:
 *
 *   max_error       int4   — current max_error stored in meta page
 *   total_tuples    int8   — total live tuples across all segments
 *   num_segments    int4   — number of directory segments
 *   freelist_pages  int4   — pages currently on the freelist
 *   tracked_pages   int4   — total pages under ref-count tracking
 * ----------------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(fiting_index_meta);

Datum
fiting_index_meta(PG_FUNCTION_ARGS)
{
	Oid					indexoid = PG_GETARG_OID(0);
	Relation			index;
	FitingMetaPageData	meta;
	int32			   *counts;
	int					freelist_pages = 0;
	BlockNumber			blkno;
	TupleDesc			tupdesc;
	Datum				values[5];
	bool				nulls[5];
	HeapTuple			tuple;

	/*
	 * Build the return tuple descriptor for the composite type.
	 * (5 columns matching the SQL RETURNS TABLE definition.)
	 */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("fiting_index_meta: function must be called as a record")));

	index = relation_open(indexoid, AccessShareLock);

	counts = palloc0(FITING_META_MAX_PAGES * sizeof(int32));
	meta   = fiting_read_meta_and_counts(index, counts);

	/* Count pages on the freelist by walking it */
	blkno = meta.freelist_head;
	while (blkno != InvalidBlockNumber && freelist_pages < FITING_META_MAX_PAGES)
	{
		Buffer	buf;
		Page	page;

		buf   = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page  = BufferGetPage(buf);
		blkno = FitingFreePage_GetNext(page);
		UnlockReleaseBuffer(buf);
		freelist_pages++;
	}

	pfree(counts);
	relation_close(index, AccessShareLock);

	memset(nulls, 0, sizeof(nulls));
	values[0] = Int32GetDatum(meta.max_error);
	values[1] = Int64GetDatum(meta.total_tuples);
	values[2] = Int32GetDatum(meta.num_segments);
	values[3] = Int32GetDatum(freelist_pages);
	values[4] = Int32GetDatum(meta.num_tracked_pages);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
