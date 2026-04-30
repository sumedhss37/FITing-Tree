/*-------------------------------------------------------------------------
 *
 * fiting_tree.h
 *   FITing-Tree learned index — shared header.
 *
 * CS349 Checkpoint 2
 * Team: Harshit Raj, Sumedh S S, Suthar Harshul, Ridam Jain
 *
 * Redesign (shared-page fix + non-contiguous segments):
 *   - Segment pages described by a linked list of FitingPageListNode records
 *     stored in a pool on the directory page.  No longer assumed contiguous.
 *   - Per-page ref-counts in the meta page (page_tuple_counts[]).  A page
 *     is only physically freed when its count drops to 0, so a page shared
 *     between two segments at bulk-build time is not wiped when one segment
 *     is resegmented.
 *   - fiting_resegment() frees old pages BEFORE allocating new ones ("free
 *     first, allocate next"), so freed pages re-enter the freelist and can
 *     be reused without extending the file.
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
 * Meta page ref-count array capacity.
 *
 * The meta page's usable area = BLCKSZ − PageHeader − opaque
 *   = 8192 − 24 − 8 = 8160 bytes.
 * Fixed fields consume 32 bytes; remaining 8128 bytes hold int32 counts.
 *   8128 / 4 = 2032 pages.
 */
#define FITING_META_MAX_PAGES  2032

/*
 * Directory page capacity.
 *
 * FitingPageListNode is now 16 bytes (added page_start_key).
 * Dir page usable area = 8160 bytes, laid out as:
 *   FitingDirHeader          (  16 bytes)
 *   FitingDirEntry[90]       (3600 bytes)  — 90 × 40
 *   FitingPageListNode[284]  (4544 bytes)  — 284 × 16
 *   Total                    8160 bytes  ≤ 8160  ✓
 */
#define FITING_DIR_MAX_SEGS   90
#define FITING_DIR_MAX_NODES  284

/*
 * Hybrid index: BTREE segment classification window.
 *
 * If the number of ShrinkingCone segments produced over a window of
 * FITING_BTREE_WINDOW consecutive tuples exceeds FITING_BTREE_SEG_THRESH,
 * those segments are merged into a single BTREE directory entry (looked up
 * by linear in-memory walk + 1 ReadBuffer instead of the learned prediction).
 *
 * Window = 8 leaf pages worth of tuples (a natural B-Tree extent).
 * Threshold = window / 100  ≈ 40 segments per 4080 tuples, i.e. average
 * segment size < 100 tuples → the linear model saves nothing useful.
 */
#define FITING_BTREE_WINDOW      (8 * FITING_TUPLES_PER_PAGE)
#define FITING_BTREE_SEG_THRESH  (FITING_BTREE_WINDOW / 100)

/* Segment type constants stored in FitingDirEntry.seg_info bit 31 */
#define FITING_SEG_TYPE_FITING  0
#define FITING_SEG_TYPE_BTREE   1

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

/* -----------------------------------------------------------------------
 * Block 0: meta page — fixed-field header (32 bytes).
 *
 * Immediately after this struct, the meta page holds:
 *   int32 page_tuple_counts[FITING_META_MAX_PAGES]
 * accessed via FitingMetaGetCounts(page).
 *
 * page_tuple_counts[blkno − FITING_DATA_START_BLKNO] counts how many
 * segments reference that block.  fiting_free_page() only wipes a page
 * when its count drops to 0.
 * ----------------------------------------------------------------------- */
typedef struct FitingMetaPageData
{
	uint32		magic;
	int32		max_error;
	int64		total_tuples;		/* total LIVE (key, TID) pairs */
	int32		num_segments;
	BlockNumber	dir_blkno;			/* always FITING_DIR_BLKNO */
	BlockNumber	freelist_head;		/* recycled-page chain; InvalidBlockNumber=empty */
	int32		num_tracked_pages;	/* high-water-mark index into page_tuple_counts */
	/* sizeof = 32 bytes */
} FitingMetaPageData;

/* -----------------------------------------------------------------------
 * Block 1: directory page.
 *
 * Layout (all at PageGetContents() base):
 *   FitingDirHeader          hdr;
 *   FitingDirEntry           entries[FITING_DIR_MAX_SEGS];
 *   FitingPageListNode       pool[FITING_DIR_MAX_NODES];
 *
 * Segments' data pages are now described by a linked list of
 * FitingPageListNode records allocated from the pool above.
 * ----------------------------------------------------------------------- */
typedef struct FitingDirHeader
{
	int32	num_segments;	/* segments currently stored */
	int32	pool_size;		/* nodes ever allocated (0-indexed high-water mark) */
	int32	pool_freelist;	/* head of free-node chain; -1 = empty */
	int32	pad;
} FitingDirHeader;			/* sizeof = 16 bytes */

/*
 * Per-segment directory entry.
 *
 * page_list_head: index into FitingPageListNode pool of the first page node;
 *   -1 = segment has no data pages yet.
 *
 * start_slot: slot offset within the FIRST page of this segment.
 *   Non-zero only when the segment's first tuple is packed mid-page during
 *   initial bulk build (i.e. it shares that page with the previous segment).
 *   Always 0 after any fiting_resegment() call.
 *
 * seg_info: packed field —
 *   bit 31      : segment type  (0 = FITING_SEG_TYPE_FITING,
 *                                1 = FITING_SEG_TYPE_BTREE)
 *   bits 30..0  : number of deleted (tombstone) tuples in data pages
 *
 * For BTREE segments, slope is set to 0.0 (unused during lookup).
 *
 * sizeof = 40 bytes (no implicit padding with this field ordering).
 */
typedef struct FitingDirEntry
{
	int64		start_key;			/*  8  @0  */
	double		slope;				/*  8  @8  */
	int32		page_list_head;		/*  4  @16 */
	int32		seg_total_tuples;	/*  4  @20 */
	int32		seg_info;			/*  4  @24  bit31=type | bits30-0=num_deleted */
	BlockNumber	buf_blkno;			/*  4  @28 */
	int32		num_buffer_tuples;	/*  4  @32 */
	int32		start_slot;			/*  4  @36 */
} FitingDirEntry;					/* sizeof = 40 bytes */

/* Segment type accessors — read/write FitingDirEntry.seg_info */
#define FitingSegType(seg) \
	(((seg)->seg_info >> 31) & 1)
#define FitingSegNumDeleted(seg) \
	((seg)->seg_info & 0x7FFFFFFF)
#define FitingSegSetType(seg, t) \
	((seg)->seg_info = ((seg)->seg_info & 0x7FFFFFFF) | ((int32)(t) << 31))
#define FitingSegAddDeleted(seg, n) \
	((seg)->seg_info += (n))

/* One node in a segment's page linked list */
typedef struct FitingPageListNode
{
	BlockNumber	page_no;			/* 4 */
	int32		next;				/* 4 — next pool index; -1 = end of list */
	int64		page_start_key;		/* 8 — first key stored on this page.
									 * Used by BTREE segments to locate the
									 * target page with 1 ReadBuffer. Always
									 * populated (for FITING segments the field
									 * exists but is not used at lookup time). */
} FitingPageListNode;				/* sizeof = 16 bytes */

/*
 * Typed overlay for the full directory page content.
 * sizeof = 16 + 90×40 + 284×16 = 8160 bytes = FITING_PAGE_USABLE (8160).
 */
typedef struct FitingDirPageContent
{
	FitingDirHeader		hdr;
	FitingDirEntry		entries[FITING_DIR_MAX_SEGS];
	FitingPageListNode	pool[FITING_DIR_MAX_NODES];
} FitingDirPageContent;		/* sizeof = 8112 bytes */

/* Blocks 2+: data / buffer pages — dense array of FitingLeafTuple ---------- */
typedef struct FitingLeafTuple
{
	int64			key;		/* 8 bytes */
	ItemPointerData	tid;		/* 6 bytes */
	uint16			flags;		/* 2 bytes — FITING_LEAF_DELETED = 0x0001 */
	/* sizeof = 16 bytes */
} FitingLeafTuple;

/* -----------------------------------------------------------------------
 * In-memory segment (produced by ShrinkingCone during build/resegment)
 * ----------------------------------------------------------------------- */
typedef struct FitingSegment
{
	int64		start_key;
	double		slope;
	int64		base_rank;	/* used only during build/resegment; not stored on disk */
} FitingSegment;

/* -----------------------------------------------------------------------
 * Scan opaque state
 * ----------------------------------------------------------------------- */
typedef struct FitingScanOpaqueData
{
	int64				search_key;
	bool				data_done;
	bool				buf_done;
	FitingDirPageContent *dir_copy;	/* palloc'd copy of full dir page; NULL until
									 * first gettuple; freed in endscan */
	int					seg_idx;	/* segment found in Phase 1 (-1 = not found) */
	int					num_segs;	/* cached from dir header */
	int32				max_error;	/* cached from meta */
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

/* How many leaf tuples fit on one data page (= 510 with 8 kB pages) */
#define FITING_TUPLES_PER_PAGE \
	((int)(FITING_PAGE_USABLE / sizeof(FitingLeafTuple)))

/* Typed pointer to page content areas */
#define FitingPageGetMeta(page) \
	((FitingMetaPageData *) PageGetContents(page))

/*
 * Access the page_tuple_counts array that immediately follows the fixed
 * FitingMetaPageData fields within the meta page content.
 */
#define FitingMetaGetCounts(page) \
	((int32 *)((char *) PageGetContents(page) + sizeof(FitingMetaPageData)))

#define FitingPageGetDirContent(page) \
	((FitingDirPageContent *) PageGetContents(page))

#define FitingPageGetLeaf(page) \
	((FitingLeafTuple *) PageGetContents(page))

/*
 * Number of FitingLeafTuples written on a page, derived from pd_lower.
 * Valid only on FITING_F_LEAF pages initialised by FitingInitPage().
 */
#define FitingPageGetNumTuples(page) \
	((int)(((PageHeader)(page))->pd_lower - SizeOfPageHeaderData) \
	 / (int)sizeof(FitingLeafTuple))

/*
 * Free-page convention: first sizeof(BlockNumber) bytes of PageGetContents()
 * hold the next free page block number (InvalidBlockNumber = last in chain).
 */
#define FitingFreePage_GetNext(page) \
	(*((BlockNumber *) PageGetContents(page)))

/* -----------------------------------------------------------------------
 * Per-index storage options (reloptions).
 *
 * Created by fiting_options() and stored in index->rd_options.
 * When no options are specified, rd_options is NULL; all callers fall
 * back to the corresponding compile-time defaults above.
 * ----------------------------------------------------------------------- */
typedef struct FitingOptions
{
	int32	vl_len_;			/* varlena header — managed by PG core */
	int32	max_error;			/* default: FITING_DEFAULT_MAX_ERROR  */
	int32	btree_window_size;	/* default: FITING_BTREE_WINDOW       */
	int32	btree_seg_thresh;	/* default: FITING_BTREE_SEG_THRESH   */
} FitingOptions;

/*
 * Retrieve per-index options from rd_options.  Returns NULL when the index
 * was created without any WITH (...) clause; callers must fall back to the
 * compile-time constants in that case.
 */
#define FitingGetOptions(rel) \
	((FitingOptions *) (rel)->rd_options)

/* -----------------------------------------------------------------------
 * Function declarations
 * ----------------------------------------------------------------------- */

/* ft_utils.c */
extern void FitingInitPage(Page page, uint16 flags);

extern FitingSegment *FitingRunShrinkingCone(int64 *keys, int64 n,
											  int32 max_error,
											  int *num_segments_out);

/* Directory node-pool helpers (operate on an in-memory FitingDirPageContent) */
extern int  fiting_dir_alloc_node(FitingDirPageContent *dir);
extern void fiting_dir_free_node(FitingDirPageContent *dir, int node_idx);

/*
 * FitingDatumGetKey — extract index key as int64 from a Datum.
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

/* ft_build.c — canonical page I/O helpers used by all sub-modules */
extern FitingMetaPageData fiting_read_meta_and_counts(Relation index,
													   int32 *counts_out);
extern void fiting_write_meta_and_counts(Relation index,
										  const FitingMetaPageData *meta,
										  const int32 *counts);
extern FitingDirPageContent *fiting_read_dir_copy(Relation index);
extern void fiting_write_dir_page(Relation index,
								   const FitingDirPageContent *dir);

/* ft_build.c — build entry points */
extern IndexBuildResult *fiting_build(Relation heap, Relation index,
									   struct IndexInfo *indexInfo);
extern void fiting_buildempty(Relation index);

/*
 * Page-management helpers.  counts[] is the in-memory copy of
 * page_tuple_counts[] from the meta page; callers must write it back
 * (via fiting_write_meta_and_counts) after any alloc/free sequence.
 */
extern BlockNumber fiting_alloc_page(Relation index,
									  FitingMetaPageData *meta,
									  int32 *counts);
extern void fiting_free_page(Relation index,
							  FitingMetaPageData *meta,
							  int32 *counts,
							  BlockNumber blkno);
extern void fiting_resegment(Relation index,
							  FitingMetaPageData *meta,
							  int32 *counts,
							  FitingDirPageContent *dir,
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

/* ft_options.c */
extern void    fiting_options_init(void);
extern bytea  *fiting_options(Datum reloptions, bool validate);

/* ft_info.c — SQL-callable monitoring functions */
extern Datum   fiting_index_info(struct FunctionCallInfoBaseData *fcinfo);
extern Datum   fiting_index_meta(struct FunctionCallInfoBaseData *fcinfo);

#endif							/* FITING_TREE_H */
