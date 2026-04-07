/*-------------------------------------------------------------------------
 *
 * ft_vacuum.c
 *   FITing-Tree VACUUM stubs for Checkpoint 1.
 *
 * Proper VACUUM support (marking TIDs dead, compacting segments) is future
 * work.  For now we return empty / zeroed statistics so that PostgreSQL's
 * autovacuum machinery does not crash when it visits the index.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "commands/vacuum.h"
#include "fiting_tree.h"

/*
 * fiting_bulkdelete
 *
 * Called during VACUUM to delete index entries whose heap TIDs are listed
 * by the callback.  For checkpoint 1 we do nothing and return zeroed stats.
 */
IndexBulkDeleteResult *
fiting_bulkdelete(IndexVacuumInfo *info,
				  IndexBulkDeleteResult *stats,
				  IndexBulkDeleteCallback callback,
				  void *callback_state)
{
	if (stats == NULL)
		stats = palloc0(sizeof(IndexBulkDeleteResult));

	/* Nothing deleted — all counts remain at zero */
	return stats;
}

/*
 * fiting_vacuumcleanup
 *
 * Post-VACUUM cleanup.  Again a no-op for checkpoint 1.
 */
IndexBulkDeleteResult *
fiting_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	if (stats == NULL)
		stats = palloc0(sizeof(IndexBulkDeleteResult));

	return stats;
}
