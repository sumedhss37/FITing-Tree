/*-------------------------------------------------------------------------
 *
 * ft_cost.c
 *   FITing-Tree cost estimation for the query planner.
 *
 * The FITing-Tree lookup cost is roughly:
 *   - 1 page read for meta
 *   - 1 page read for the directory (binary search in memory)
 *   - O(log(max_error)) page reads for the binary search in leaf data
 *       ≈ log2(64) = 6 reads for max_error = 32
 *
 * For checkpoint 1 we use genericcostestimate as a baseline and then
 * patch the page count to reflect the actual I/O profile.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fiting_tree.h"
#include "utils/selfuncs.h"

void
fiting_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
					Cost *indexStartupCost, Cost *indexTotalCost,
					Selectivity *indexSelectivity, double *indexCorrelation,
					double *indexPages)
{
	GenericCosts costs = {0};

	/*
	 * A FITing-Tree point lookup touches:
	 *   meta page (1) + dir page (1) + ~log2(max_error) leaf pages (~6)
	 * We report 8 as a conservative estimate.
	 */
	costs.numIndexPages = 8;

	/* Selectivity and tuple count are handled by genericcostestimate */
	genericcostestimate(root, path, loop_count, &costs);

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost   = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages       = costs.numIndexPages;
}
