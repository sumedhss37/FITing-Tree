/*-------------------------------------------------------------------------
 *
 * fiting_tree.h
 *   FITing-Tree learned index — shared header.
 *
 * CS349 Checkpoint 1
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
#include "nodes/pathnodes.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/relcache.h"

/* -----------------------------------------------------------------------
 * Constants
 * ----------------------------------------------------------------------- */

#define FITING_MAGIC             0xF1771234
#define FITING_DEFAULT_MAX_ERROR 32
#define FITING_PAGE_ID           0xFF84

/* Fixed block layout (checkpoint 1 — single directory page) */
#define FITING_META_BLKNO        ((BlockNumber) 0)
#define FITING_DIR_BLKNO         ((BlockNumber) 1)
#define FITING_DATA_START_BLKNO  ((BlockNumber) 2)

/* Page flag bits stored in FitingPageOpaqueData.flags */
#define FITING_F_META   0x0001
#define FITING_F_DIR    0x0002
#define FITING_F_LEAF   0x0004

/* -----------------------------------------------------------------------
 * On-disk page structures
 * ----------------------------------------------------------------------- */

/*
 * Opaque area at the end of every FITing-Tree page.
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
	int64		total_tuples;	/* total (key, TID) pairs in the index */
	int32		num_segments;	/* number of linear segments */
	BlockNumber dir_blkno;		/* always FITING_DIR_BLKNO for checkpoint 1 */
} FitingMetaPageData;

/* Block 1: directory page — array of FitingDirEntry ------------------- */
typedef struct FitingDirEntry
{
	int32		start_key;		/* first key in this segment */
	int32		pad;			/* explicit padding for double alignment */
	double		slope;			/* rank/key-delta linear slope */
	int64		base_rank;		/* rank of start_key */
} FitingDirEntry;

/* Blocks 2+: leaf data pages — dense array of FitingLeafTuple --------- */
typedef struct FitingLeafTuple
{
	int32			key;		/* 4 bytes */
	ItemPointerData	tid;		/* 6 bytes — heap TID */
	/* 2 bytes compiler padding → sizeof = 12 */
} FitingLeafTuple;

/* -----------------------------------------------------------------------
 * In-memory segment (produced by ShrinkingCone during build)
 * ----------------------------------------------------------------------- */
typedef struct FitingSegment
{
	int32		start_key;
	double		slope;
	int64		base_rank;
} FitingSegment;

/* -----------------------------------------------------------------------
 * Scan opaque state
 * ----------------------------------------------------------------------- */
typedef struct FitingScanOpaqueData
{
	bool		done;			/* true once we've returned the (single) result */
	int32		search_key;		/* equality key from ScanKey */
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

/* Usable bytes per page (header + opaque stripped) */
#define FITING_PAGE_OPAQUE_SIZE  MAXALIGN(sizeof(FitingPageOpaqueData))
#define FITING_PAGE_USABLE \
	(BLCKSZ - SizeOfPageHeaderData - FITING_PAGE_OPAQUE_SIZE)

/* How many leaf tuples fit on one data page */
#define FITING_TUPLES_PER_PAGE \
	((int)(FITING_PAGE_USABLE / sizeof(FitingLeafTuple)))

/* How many dir entries fit on one directory page */
#define FITING_DIR_ENTRIES_MAX \
	((int)(FITING_PAGE_USABLE / sizeof(FitingDirEntry)))

/* Get typed pointer to page contents */
#define FitingPageGetMeta(page) \
	((FitingMetaPageData *) PageGetContents(page))

#define FitingPageGetDir(page) \
	((FitingDirEntry *) PageGetContents(page))

#define FitingPageGetLeaf(page) \
	((FitingLeafTuple *) PageGetContents(page))

/* -----------------------------------------------------------------------
 * Function declarations
 * ----------------------------------------------------------------------- */

/* ft_utils.c */
extern void FitingInitPage(Page page, uint16 flags);

extern FitingSegment *FitingRunShrinkingCone(int32 *keys, int64 n,
											  int32 max_error,
											  int *num_segments_out);

/* ft_build.c */
extern IndexBuildResult *fiting_build(Relation heap, Relation index,
									   struct IndexInfo *indexInfo);
extern void fiting_buildempty(Relation index);

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
