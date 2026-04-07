/*-------------------------------------------------------------------------
 *
 * ft_insert.c
 *   FITing-Tree aminsert — stub for Checkpoint 1.
 *
 * Dynamic inserts require delta buffers and local segment re-fitting.
 * That is out of scope for the checkpoint submission.  Any attempt to
 * insert a new row into a table that has a fiting_tree index will produce
 * a clear error message.
 *
 * Workflow for Checkpoint 1:
 *   1. Bulk-load data into the table.
 *   2. CLUSTER the table on the indexed column.
 *   3. CREATE INDEX ... USING fiting.
 *   4. Query with SELECT ... WHERE key = <value>.
 *   (Do not INSERT after step 3.)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "fiting_tree.h"

bool
fiting_insert(Relation indexRelation,
			  Datum *values,
			  bool *isnull,
			  ItemPointer heap_tid,
			  Relation heapRelation,
			  IndexUniqueCheck checkUnique,
			  bool indexUnchanged,
			  struct IndexInfo *indexInfo)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("fiting_tree does not support row-level inserts in "
					"checkpoint 1"),
			 errhint("Bulk-load data before creating the index, then use "
					 "CREATE INDEX ... USING fiting.")));
	return false;				/* unreachable */
}
