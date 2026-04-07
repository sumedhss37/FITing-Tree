/*-------------------------------------------------------------------------
 *
 * ft_utils.c
 *   FITing-Tree: page initialization and ShrinkingCone algorithm.
 *
 * ShrinkingCone (Galakatos et al., SIGMOD 2019):
 *   Streams over a sorted key array, maintaining a "cone" of valid slopes
 *   for the current linear segment.  When a new key forces the cone to
 *   collapse (no slope satisfies the ±MAX_ERROR bound for all seen keys),
 *   the current segment is finalised and a new one starts.  The result is
 *   the minimum number of piecewise-linear segments that bound every key's
 *   predicted rank within ±max_error of its true rank.
 *
 * Assumptions (Checkpoint 1):
 *   - Keys are int4 and the input array is sorted ascending.
 *   - Ranks are implicit: rank(i) = i (unique or near-unique keys).
 *   - Duplicate key runs are handled conservatively (cone not updated when
 *     key delta is zero), so at most max_error duplicates are safe.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fiting_tree.h"
#include "storage/bufpage.h"

/* Large-but-finite stand-in for ±∞ in slope arithmetic */
#define SLOPE_INF  1e15

/* Forward declaration */
static double finalise_slope(double cone_lo, double cone_hi);

/* -----------------------------------------------------------------------
 * FitingInitPage
 *
 * Initialise a raw 8 KB page for any FITing-Tree page type.  The opaque
 * area is placed at pd_special (end of page) by PageInit.
 * ----------------------------------------------------------------------- */
void
FitingInitPage(Page page, uint16 flags)
{
	FitingPageOpaque opaque;

	PageInit(page, BLCKSZ, sizeof(FitingPageOpaqueData));

	opaque = FitingPageGetOpaque(page);
	opaque->flags = flags;
	opaque->page_id = FITING_PAGE_ID;
}

/* -----------------------------------------------------------------------
 * FitingRunShrinkingCone
 *
 * Core algorithm.  Takes a sorted int32 key array of length n and returns
 * an array of FitingSegment (palloced).  *num_segments_out receives the
 * count.
 *
 * Each output segment satisfies:
 *   |base_rank + (key - start_key) * slope  -  actual_rank(key)|  ≤ max_error
 * for every key in that segment.
 * ----------------------------------------------------------------------- */
FitingSegment *
FitingRunShrinkingCone(int32 *keys, int64 n, int32 max_error,
					   int *num_segments_out)
{
	FitingSegment *segs;
	int			segs_alloc;
	int			num_segs;

	int64		origin_rank;
	int32		origin_key;
	double		cone_lo;		/* lower bound on valid slope */
	double		cone_hi;		/* upper bound on valid slope */

	/* Handle trivially empty index */
	if (n == 0)
	{
		*num_segments_out = 0;
		return NULL;
	}

	segs_alloc = 64;
	segs = palloc(segs_alloc * sizeof(FitingSegment));
	num_segs = 0;

	/* Start first segment at key[0] with rank 0 */
	origin_key = keys[0];
	origin_rank = 0;
	cone_lo = -SLOPE_INF;
	cone_hi = +SLOPE_INF;

	for (int64 i = 1; i < n; i++)
	{
		int32		ki = keys[i];
		int64		ri = i;
		double		dk;
		double		s_hi, s_lo;
		double		new_lo, new_hi;

		/* Duplicate key: rank increases but key delta is 0.
		 * Cannot compute a slope — skip cone update and continue. */
		if (ki == origin_key)
			continue;

		dk = (double) (ki - origin_key);
		s_hi = ((double) (ri + max_error - origin_rank)) / dk;
		s_lo = ((double) (ri - max_error - origin_rank)) / dk;

		new_hi = (s_hi < cone_hi) ? s_hi : cone_hi;
		new_lo = (s_lo > cone_lo) ? s_lo : cone_lo;

		if (new_lo > new_hi)
		{
			/*
			 * Cone collapsed: the current segment cannot accommodate key[i]
			 * within max_error.  Finalise the segment ending at key[i-1].
			 */
			double		slope = finalise_slope(cone_lo, cone_hi);

			/* Grow segment array if needed */
			if (num_segs >= segs_alloc)
			{
				segs_alloc *= 2;
				segs = repalloc(segs, segs_alloc * sizeof(FitingSegment));
			}
			segs[num_segs].start_key = origin_key;
			segs[num_segs].slope = slope;
			segs[num_segs].base_rank = origin_rank;
			num_segs++;

			/* New origin is key[i-1] */
			origin_key = keys[i - 1];
			origin_rank = i - 1;
			cone_lo = -SLOPE_INF;
			cone_hi = +SLOPE_INF;

			/* Re-process key[i] from the new origin */
			if (ki == origin_key)
				continue;		/* duplicate of new origin */

			dk = (double) (ki - origin_key);
			s_hi = ((double) (ri + max_error - origin_rank)) / dk;
			s_lo = ((double) (ri - max_error - origin_rank)) / dk;
			new_lo = s_lo;
			new_hi = s_hi;
		}

		cone_lo = new_lo;
		cone_hi = new_hi;
	}

	/* Finalise the last (open) segment */
	if (num_segs >= segs_alloc)
	{
		segs_alloc++;
		segs = repalloc(segs, segs_alloc * sizeof(FitingSegment));
	}
	segs[num_segs].start_key = origin_key;
	segs[num_segs].slope = finalise_slope(cone_lo, cone_hi);
	segs[num_segs].base_rank = origin_rank;
	num_segs++;

	*num_segments_out = num_segs;
	return segs;
}

/*
 * Choose the segment slope as the midpoint of the valid cone.
 * The midpoint minimises worst-case error within the segment.
 * Handle degenerate cases (infinite bounds) gracefully.
 */
static double
finalise_slope(double cone_lo, double cone_hi)
{
	bool		lo_inf = (cone_lo <= -SLOPE_INF);
	bool		hi_inf = (cone_hi >= SLOPE_INF);

	if (lo_inf && hi_inf)
		return 1.0;				/* single-point segment; slope doesn't matter */
	if (lo_inf)
		return cone_hi;
	if (hi_inf)
		return cone_lo;
	return (cone_lo + cone_hi) / 2.0;
}
