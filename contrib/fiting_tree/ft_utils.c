/*-------------------------------------------------------------------------
 *
 * ft_utils.c
 *   FITing-Tree: page initialization, ShrinkingCone algorithm, and
 *   directory node-pool helpers.
 *
 * ShrinkingCone (Galakatos et al., SIGMOD 2019):
 *   Streams over a sorted key array, maintaining a "cone" of valid slopes
 *   for the current linear segment.  When a new key forces the cone to
 *   collapse (no slope satisfies the ±MAX_ERROR bound for all seen keys),
 *   the current segment is finalised and a new one starts.  The result is
 *   the minimum number of piecewise-linear segments that bound every key's
 *   predicted rank within ±max_error of its true rank.
 *
 * Node-pool helpers:
 *   fiting_dir_alloc_node / fiting_dir_free_node operate on the in-memory
 *   FitingDirPageContent copy passed by the caller.  The caller is
 *   responsible for writing the modified dir copy back to disk.
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
 * Initialise a raw 8 KB page for any FITing-Tree page type.
 * ----------------------------------------------------------------------- */
void
FitingInitPage(Page page, uint16 flags)
{
	FitingPageOpaque opaque;

	PageInit(page, BLCKSZ, sizeof(FitingPageOpaqueData));

	opaque = FitingPageGetOpaque(page);
	opaque->flags   = flags;
	opaque->page_id = FITING_PAGE_ID;
}

/* -----------------------------------------------------------------------
 * fiting_dir_alloc_node
 *
 * Allocate a free node from the pool in dir.  Returns the node index.
 * Pops from pool_freelist first; otherwise claims the next unused slot.
 * Caller must write the modified dir copy back to disk.
 * ----------------------------------------------------------------------- */
int
fiting_dir_alloc_node(FitingDirPageContent *dir)
{
	int		idx;

	if (dir->hdr.pool_freelist >= 0)
	{
		/* Pop head of free list */
		idx = dir->hdr.pool_freelist;
		dir->hdr.pool_freelist = dir->pool[idx].next;
		dir->pool[idx].page_no = InvalidBlockNumber;
		dir->pool[idx].next    = -1;
		return idx;
	}

	if (dir->hdr.pool_size >= FITING_DIR_MAX_NODES)
		elog(ERROR,
			 "fiting_tree: directory node pool exhausted (max %d nodes). "
			 "Rebuild the index.",
			 FITING_DIR_MAX_NODES);

	idx = dir->hdr.pool_size++;
	dir->pool[idx].page_no = InvalidBlockNumber;
	dir->pool[idx].next    = -1;
	return idx;
}

/* -----------------------------------------------------------------------
 * fiting_dir_free_node
 *
 * Return node_idx to the free list in dir.
 * Caller must write the modified dir copy back to disk.
 * ----------------------------------------------------------------------- */
void
fiting_dir_free_node(FitingDirPageContent *dir, int node_idx)
{
	Assert(node_idx >= 0 && node_idx < FITING_DIR_MAX_NODES);
	dir->pool[node_idx].page_no = InvalidBlockNumber;
	dir->pool[node_idx].next    = dir->hdr.pool_freelist;
	dir->hdr.pool_freelist      = node_idx;
}

/* -----------------------------------------------------------------------
 * FitingRunShrinkingCone
 *
 * Core algorithm.  Takes a sorted int64 key array of length n and returns
 * an array of FitingSegment (palloced).  *num_segments_out receives the count.
 * ----------------------------------------------------------------------- */
FitingSegment *
FitingRunShrinkingCone(int64 *keys, int64 n, int32 max_error,
					   int *num_segments_out)
{
	FitingSegment *segs;
	int			segs_alloc;
	int			num_segs;

	int64		origin_rank;
	int64		origin_key;
	double		cone_lo;
	double		cone_hi;

	if (n == 0)
	{
		*num_segments_out = 0;
		return NULL;
	}

	segs_alloc = 64;
	segs = palloc(segs_alloc * sizeof(FitingSegment));
	num_segs = 0;

	origin_key  = keys[0];
	origin_rank = 0;
	cone_lo     = -SLOPE_INF;
	cone_hi     = +SLOPE_INF;

	for (int64 i = 1; i < n; i++)
	{
		int64	ki = keys[i];
		int64	ri = i;
		double	dk;
		double	s_hi, s_lo;
		double	new_lo, new_hi;

		if (ki == origin_key)
			continue;

		dk   = (double) (ki - origin_key);
		s_hi = ((double) (ri + max_error - origin_rank)) / dk;
		s_lo = ((double) (ri - max_error - origin_rank)) / dk;

		new_hi = (s_hi < cone_hi) ? s_hi : cone_hi;
		new_lo = (s_lo > cone_lo) ? s_lo : cone_lo;

		if (new_lo > new_hi)
		{
			double slope = finalise_slope(cone_lo, cone_hi);

			if (num_segs >= segs_alloc)
			{
				segs_alloc *= 2;
				segs = repalloc(segs, segs_alloc * sizeof(FitingSegment));
			}
			segs[num_segs].start_key  = origin_key;
			segs[num_segs].slope      = slope;
			segs[num_segs].base_rank  = origin_rank;
			num_segs++;

			origin_key  = keys[i - 1];
			origin_rank = i - 1;
			cone_lo     = -SLOPE_INF;
			cone_hi     = +SLOPE_INF;

			if (ki == origin_key)
				continue;

			dk   = (double) (ki - origin_key);
			s_hi = ((double) (ri + max_error - origin_rank)) / dk;
			s_lo = ((double) (ri - max_error - origin_rank)) / dk;
			new_lo = s_lo;
			new_hi = s_hi;
		}

		cone_lo = new_lo;
		cone_hi = new_hi;
	}

	/* Finalise the last open segment */
	if (num_segs >= segs_alloc)
	{
		segs_alloc++;
		segs = repalloc(segs, segs_alloc * sizeof(FitingSegment));
	}
	segs[num_segs].start_key = origin_key;
	segs[num_segs].slope     = finalise_slope(cone_lo, cone_hi);
	segs[num_segs].base_rank = origin_rank;
	num_segs++;

	*num_segments_out = num_segs;
	return segs;
}

static double
finalise_slope(double cone_lo, double cone_hi)
{
	bool lo_inf = (cone_lo <= -SLOPE_INF);
	bool hi_inf = (cone_hi >=  SLOPE_INF);

	if (lo_inf && hi_inf)  return 1.0;
	if (lo_inf)            return cone_hi;
	if (hi_inf)            return cone_lo;
	return (cone_lo + cone_hi) / 2.0;
}
