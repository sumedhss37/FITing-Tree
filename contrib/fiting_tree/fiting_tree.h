/*-------------------------------------------------------------------------
 *
 * fiting_tree.h
 *   FITing-Tree learned index — shared header.
 *
 * CS349 Checkpoint 2
 * Team: Harshit Raj, Sumedh S S, Suthar Harshul, Ridam Jain
 *
 *-------------------------------------------------------------------------
 */
#ifndef FITING_TREE_H
#define FITING_TREE_H

#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/itup.h"
#include "access/relscan.h"
#include "catalog/pg_type_d.h"	/* INT4OID, TIMESTAMPOID, TIMESTAMPTZOID */
#include "nodes/pathnodes.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"	/* Timestamp, DatumGetTimestamp */

/* -----------------------------------------------------------------------
 * Constants
 * ----------------------------------------------------------------------- */

#define FITING_MAGIC             0xF1771234
#define FITING_DEFAULT_MAX_ERROR 32
#define FITING_PAGE_ID           0xFF84

/* Fixed block layout */
#define FITING_META_BLKNO        ((BlockNumber) 0)
#define FITING_DIR_BLKNO         ((BlockNumber) 1)
#define FITING_DATA_START_BLKNO  ((BlockNumber) 2)

/* Page flag bits stored in FitingPageOpaqueData.flags */
#define FITING_F_META   0x0001
#define FITING_F_DIR    0x0002
#define FITING_F_LEAF   0x0004
#define FITING_F_FREE   0x0008	/* page is on the free list */

/* FitingLeafTuple.flags bits */
#define FITING_LEAF_DELETED  0x0001	/* tombstone — set by ambulkdelete */

/*
 * Segment data pages are always allocated contiguously (ExtendBufferedRel
 * only, no freelist), so a segment's physical pages are:
 *   first_data_blkno, first_data_blkno+1, ..., first_data_blkno+num_data_pages-1
 * No per-segment page-count limit — segments can be as large as the dataset.
 */

/* -----------------------------------------------------------------------
 * On-disk page structures
 * ----------------------------------------------------------------------- */

/*
 * Opaque area at the end of every FITing-Tree page (unchanged from CP1).
 * PageInit places this at pd_special; access via FitingPageGetOpaque().
 */
typedef struct FitingPageOpaqueData
{
	uint16		flags;
	uint16		page_id;		/* always FITING_PAGE_ID */
} FitingPageOpaqueData;

typedef FitingPageOpaqueData *FitingPageOpaque;

/* Block 0: meta page --------------------------------------------------- */
typedef struct FitingMetaPageData
{
	uint32		magic;
	int32		max_error;
	int64		total_tuples;	/* total LIVE (key, TID) pairs in the index */
	int32		num_segments;	/* number of linear segments */
	BlockNumber dir_blkno;		/* always FITING_DIR_BLKNO */
	BlockNumber freelist_head;	/* head of recycled-page chain;
								 * InvalidBlockNumber = empty */
	int32		pad;			/* explicit padding → sizeof = 32 bytes */
} FitingMetaPageData;

/*
 * Block 1: directory page — array of FitingDirEntry.
 *
 * Each segment's data pages are physically contiguous on disk:
 *   blkno(page p) = first_data_blkno + p
 *
 * This removes the per-segment page-count limit.  New segment pages are
 * always allocated via ExtendBufferedRel (never from the freelist), so
 * contiguity is guaranteed after every build or re-segmentation.
 *
 * Rank-to-address formula (O(1)):
 *   local_pred = (key − start_key) × slope        (local rank estimate)
 *   absolute   = start_slot + local_rank
 *   page_idx   = absolute / FITING_TUPLES_PER_PAGE
 *   slot       = absolute % FITING_TUPLES_PER_PAGE
 *   blkno      = first_data_blkno + page_idx
 */
typedef struct FitingDirEntry
{
	int64		start_key;		/* first key in this segment (int64 for all types) */
	int32		start_slot;		/* slot offset within first_data_blkno page;
								 * non-zero when segment shares its first page
								 * with the tail of the previous segment
								 * (initial packed build only).
								 * Always 0 for freshly re-segmented data. */
	double		slope;			/* local rank ≈ (key − start_key) × slope */
	BlockNumber first_data_blkno; /* first contiguous data page */
	int32		num_data_pages; /* number of contiguous data pages */
	int32		seg_total_tuples;	/* live + deleted tuples in data pages
									 * (used as rank-space upper bound) */
	int32		seg_num_deleted;	/* tombstone count */
	BlockNumber buf_blkno;		/* buffer page; InvalidBlockNumber if none */
	int32		num_buffer_tuples;	/* entries in buffer page */
	/* sizeof = 48 bytes (4 bytes implicit padding between start_slot and slope) */
} FitingDirEntry;

/* Blocks 2+: data/buffer pages — dense array of FitingLeafTuple -------- */
typedef struct FitingLeafTuple
{
	int64			key;		/* 8 bytes — int4 stored as int64; timestamp IS int64 */
	ItemPointerData	tid;		/* 6 bytes — heap TID */
	uint16			flags;		/* 2 bytes — FITING_LEAF_DELETED = 0x0001 */
	/* sizeof = 16 bytes */
} FitingLeafTuple;

/* -----------------------------------------------------------------------
 * In-memory segment (produced by ShrinkingCone during build/resegment)
 * ----------------------------------------------------------------------- */
typedef struct FitingSegment
{
	int64		start_key;	/* int64 to match FitingDirEntry.start_key */
	double		slope;
	int64		base_rank;	/* used only during build/resegment to compute
							 * start_slot and first_data_blkno; not stored on disk */
} FitingSegment;

/* -----------------------------------------------------------------------
 * Scan opaque state
 * ----------------------------------------------------------------------- */
typedef struct FitingScanOpaqueData
{
	int64			search_key;	/* int64 to hold both int4 and timestamp keys */
	bool			data_done;	/* data pages have been searched */
	bool			buf_done;	/* buffer page has been searched */
	FitingDirEntry *dir_copy;	/* palloc'd copy of directory; NULL until
								 * first gettuple; freed in endscan */
	int				seg_idx;	/* segment found in Phase 1 (-1 = not found) */
	int				num_segs;	/* cached from meta */
	int32			max_error;	/* cached from meta */
} FitingScanOpaqueData;

typedef FitingScanOpaqueData *FitingScanOpaque;

/* -----------------------------------------------------------------------
 * Page layout macros
 * ----------------------------------------------------------------------- */

#define FitingPageGetOpaque(page) \
	((FitingPageOpaque) PageGetSpecialPointer(page))

#define FitingPageIsMeta(page) \
	((FitingPageGetOpaque(page)->flags & FITING_F_META) != 0)

#define FitingPageIsDir(page) \
	((FitingPageGetOpaque(page)->flags & FITING_F_DIR) != 0)

#define FitingPageIsLeaf(page) \
	((FitingPageGetOpaque(page)->flags & FITING_F_LEAF) != 0)

#define FitingPageIsFree(page) \
	((FitingPageGetOpaque(page)->flags & FITING_F_FREE) != 0)

/* Usable bytes per page (header + opaque stripped) */
#define FITING_PAGE_OPAQUE_SIZE  MAXALIGN(sizeof(FitingPageOpaqueData))
#define FITING_PAGE_USABLE \
	(BLCKSZ - SizeOfPageHeaderData - FITING_PAGE_OPAQUE_SIZE)

/* How many leaf tuples fit on one data page (= 680 with default 8 kB pages) */
#define FITING_TUPLES_PER_PAGE \
	((int)(FITING_PAGE_USABLE / sizeof(FitingLeafTuple)))

/* How many dir entries fit on one directory page (= 170 with 48-byte entries) */
#define FITING_DIR_ENTRIES_MAX \
	((int)(FITING_PAGE_USABLE / sizeof(FitingDirEntry)))

/* Get typed pointer to page contents */
#define FitingPageGetMeta(page) \
	((FitingMetaPageData *) PageGetContents(page))

#define FitingPageGetDir(page) \
	((FitingDirEntry *) PageGetContents(page))

#define FitingPageGetLeaf(page) \
	((FitingLeafTuple *) PageGetContents(page))

/*
 * Free-page convention:
 * A recycled page has flags = FITING_F_FREE.  The first sizeof(BlockNumber)
 * bytes of PageGetContents() hold the block number of the next free page
 * (InvalidBlockNumber if this is the last one in the chain).
 */
#define FitingFreePage_GetNext(page) \
	(*((BlockNumber *) PageGetContents(page)))

/* -----------------------------------------------------------------------
 * Function declarations
 * ----------------------------------------------------------------------- */

/* ft_utils.c */
extern void FitingInitPage(Page page, uint16 flags);

extern FitingSegment *FitingRunShrinkingCone(int64 *keys, int64 n,
											  int32 max_error,
											  int *num_segments_out);

/*
 * FitingDatumGetKey — extract index key as int64 from a Datum.
 *
 * All supported types map to int64:
 *   int4      → sign-extend to int64 (lossless)
 *   timestamp → already stored as int64 (µs since epoch)
 *   timestamptz → same underlying representation as timestamp
 */
static inline int64
FitingDatumGetKey(Datum d, Oid typid)
{
	switch (typid)
	{
		case INT4OID:
			return (int64) DatumGetInt32(d);
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return (int64) DatumGetTimestamp(d);
		default:
			elog(ERROR, "fiting_tree: unsupported key type OID %u", typid);
			return 0;				/* unreachable */
	}
}

/* ft_build.c */
extern IndexBuildResult *fiting_build(Relation heap, Relation index,
									   struct IndexInfo *indexInfo);
extern void fiting_buildempty(Relation index);

/*
 * Shared page-management helpers used by ft_insert.c and ft_vacuum.c.
 * Implemented in ft_build.c (declared here for cross-file access).
 */
extern BlockNumber fiting_alloc_page(Relation index,
									  FitingMetaPageData *meta);
extern void fiting_free_page(Relation index,
							  FitingMetaPageData *meta,
							  BlockNumber blkno);
extern void fiting_resegment(Relation index,
							  FitingMetaPageData *meta,
							  FitingDirEntry *dir,
							  int seg_idx);

/* ft_scan.c */
extern IndexScanDesc fiting_beginscan(Relation indexRelation, int nkeys,
									   int norderbys);
extern void fiting_rescan(IndexScanDesc scan, ScanKey keys, int nkeys,
						   ScanKey orderbys, int norderbys);
extern bool fiting_gettuple(IndexScanDesc scan, ScanDirection direction);
extern void fiting_endscan(IndexScanDesc scan);

/* ft_insert.c */
extern bool fiting_insert(Relation indexRelation, Datum *values, bool *isnull,
						   ItemPointer heap_tid, Relation heapRelation,
						   IndexUniqueCheck checkUnique, bool indexUnchanged,
						   struct IndexInfo *indexInfo);

/* ft_vacuum.c */
extern IndexBulkDeleteResult *fiting_bulkdelete(IndexVacuumInfo *info,
												  IndexBulkDeleteResult *stats,
												  IndexBulkDeleteCallback callback,
												  void *callback_state);
extern IndexBulkDeleteResult *fiting_vacuumcleanup(IndexVacuumInfo *info,
													 IndexBulkDeleteResult *stats);

/* ft_cost.c */
extern void fiting_costestimate(PlannerInfo *root, IndexPath *path,
								 double loop_count,
								 Cost *indexStartupCost,
								 Cost *indexTotalCost,
								 Selectivity *indexSelectivity,
								 double *indexCorrelation,
								 double *indexPages);

/* ft_validate.c (inline here for simplicity) */
extern bool fiting_validate(Oid opclassoid);

#endif							/* FITING_TREE_H */
