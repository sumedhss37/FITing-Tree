/*-------------------------------------------------------------------------
 *
 * ft_options.c
 *   FITing-Tree per-index storage options (reloptions).
 *
 * All parameters have sensible defaults so the index works identically
 * with no user configuration.  Options exist purely for monitoring and
 * experimentation:
 *
 *   max_error          (int, default 32)
 *       Maximum allowed prediction error for the linear model.
 *       Smaller  → more segments, faster lookup, larger directory.
 *       Larger   → fewer segments, wider binary-search window.
 *       Takes effect only after REINDEX.
 *
 *   btree_window_size  (int, default 8 × TUPLES_PER_PAGE ≈ 4080)
 *       Tuple window over which segment density is measured to decide
 *       whether to use a BTREE segment instead of a FITING segment.
 *       Takes effect only after REINDEX (or the next resegment cycle).
 *
 *   btree_seg_thresh   (int, default 40)
 *       If the number of ShrinkingCone segments in one window exceeds
 *       this threshold, that window is collapsed into a single BTREE
 *       segment looked up via in-memory page_start_key walk + 1 ReadBuffer.
 *       Takes effect only after REINDEX (or the next resegment cycle).
 *
 * Usage:
 *   CREATE INDEX idx ON t USING fiting (col)
 *       WITH (max_error = 64, btree_window_size = 4080, btree_seg_thresh = 40);
 *
 *   ALTER  INDEX idx SET  (max_error = 128);
 *   ALTER  INDEX idx RESET(btree_window_size);   -- back to default
 *
 *   SELECT reloptions FROM pg_class WHERE relname = 'idx';
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/reloptions.h"
#include "fiting_tree.h"
#include "utils/guc.h"

/* -----------------------------------------------------------------------
 * Module-level relopt kind.  Allocated once in fiting_options_init(),
 * called from _PG_init() in ft_handler.c.
 * ----------------------------------------------------------------------- */
static relopt_kind fiting_relopt_kind;

/* -----------------------------------------------------------------------
 * fiting_options_init
 *
 * Register the three FITing-Tree reloption keys.
 * Must be called before any index with these options is created or opened.
 * ----------------------------------------------------------------------- */
void
fiting_options_init(void)
{
	fiting_relopt_kind = add_reloption_kind();

	add_int_reloption(fiting_relopt_kind,
					  "max_error",
					  "Maximum prediction error for the learned linear model. "
					  "Requires REINDEX to take effect.",
					  FITING_DEFAULT_MAX_ERROR,		/* default = 32 */
					  1,							/* minimum */
					  10000,						/* maximum */
					  AccessExclusiveLock);

	add_int_reloption(fiting_relopt_kind,
					  "btree_window_size",
					  "Tuple window (in tuples) over which segment density is "
					  "measured for BTREE/FITING classification. "
					  "Default = 8 × tuples_per_page ≈ 4080.",
					  FITING_BTREE_WINDOW,			/* default ≈ 4080 */
					  100,
					  10000000,
					  AccessExclusiveLock);

	add_int_reloption(fiting_relopt_kind,
					  "btree_seg_thresh",
					  "If the number of ShrinkingCone segments in one window "
					  "exceeds this value, those tuples become a BTREE segment. "
					  "Default = 40 (= window_size / 100).",
					  FITING_BTREE_SEG_THRESH,		/* default = 40 */
					  1,
					  100000,
					  AccessExclusiveLock);
}

/* -----------------------------------------------------------------------
 * fiting_options
 *
 * Parse and validate reloptions for a FITing-Tree index.
 * Called by PostgreSQL core whenever a relation with fiting AM reloptions
 * is opened or when CREATE/ALTER INDEX WITH (...) is processed.
 *
 * Returns a palloc'd FitingOptions struct (as bytea) stored in rd_options,
 * or NULL if reloptions is empty/NULL.
 * ----------------------------------------------------------------------- */
bytea *
fiting_options(Datum reloptions, bool validate)
{
	static const relopt_parse_elt optlist[] = {
		{
			"max_error",
			RELOPT_TYPE_INT,
			offsetof(FitingOptions, max_error)
		},
		{
			"btree_window_size",
			RELOPT_TYPE_INT,
			offsetof(FitingOptions, btree_window_size)
		},
		{
			"btree_seg_thresh",
			RELOPT_TYPE_INT,
			offsetof(FitingOptions, btree_seg_thresh)
		},
	};

	return (bytea *) build_reloptions(reloptions, validate,
									  fiting_relopt_kind,
									  sizeof(FitingOptions),
									  optlist, lengthof(optlist));
}
