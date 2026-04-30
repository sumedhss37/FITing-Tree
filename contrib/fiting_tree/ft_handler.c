/*-------------------------------------------------------------------------
 *
 * ft_handler.c
 *   FITing-Tree index AM handler — returns the IndexAmRoutine to PostgreSQL.
 *
 * The handler function is the single entry point that PostgreSQL calls to
 * discover all callbacks for the "fiting" access method.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "fmgr.h"
#include "fiting_tree.h"

PG_MODULE_MAGIC_EXT(
					.name = "fiting_tree",
					.version = PG_VERSION
);

/*
 * _PG_init — called once when the shared library is loaded.
 * Registers the FITing-Tree reloption keys so they are available before
 * any index with those options is created or opened.
 */
void
_PG_init(void)
{
	fiting_options_init();
}

PG_FUNCTION_INFO_V1(fiting_handler);

/*
 * fiting_handler — called by PostgreSQL when the "fiting" AM is referenced.
 * Returns a pointer to a static IndexAmRoutine struct describing every
 * callback and capability of the FITing-Tree index.
 */
Datum
fiting_handler(PG_FUNCTION_ARGS)
{
	static const IndexAmRoutine amroutine = {
		.type = T_IndexAmRoutine,

		/*
		 * Checkpoint 1: equality scans only (strategy 1 = "=" in our opclass).
		 * One support function: btint4cmp for key comparison.
		 */
		.amstrategies = 1,
		.amsupport = 1,
		.amoptsprocnum = 0,

		/* Capabilities */
		.amcanorder = false,		/* no ORDER BY support yet */
		.amcanorderbyop = false,
		.amcanhash = false,
		.amconsistentequality = false,
		.amconsistentordering = false,
		.amcanbackward = false,
		.amcanunique = false,		/* no UNIQUE index support */
		.amcanmulticol = false,		/* single-column only */
		.amoptionalkey = false,		/* scan always needs at least one key */
		.amsearcharray = false,
		.amsearchnulls = false,
		.amstorage = false,
		.amclusterable = true,		/* data must be physically sorted */
		.ampredlocks = false,
		.amcanparallel = false,
		.amcanbuildparallel = false,
		.amcaninclude = false,
		.amusemaintenanceworkmem = false,
		.amsummarizing = false,
		.amparallelvacuumoptions = 0,
		.amkeytype = InvalidOid,

		/* Callbacks */
		.ambuild = fiting_build,
		.ambuildempty = fiting_buildempty,
		.aminsert = fiting_insert,
		.aminsertcleanup = NULL,
		.ambulkdelete = fiting_bulkdelete,
		.amvacuumcleanup = fiting_vacuumcleanup,
		.amcanreturn = NULL,
		.amcostestimate = fiting_costestimate,
		.amgettreeheight = NULL,
		.amoptions = fiting_options,
		.amproperty = NULL,
		.ambuildphasename = NULL,
		.amvalidate = fiting_validate,
		.amadjustmembers = NULL,
		.ambeginscan = fiting_beginscan,
		.amrescan = fiting_rescan,
		.amgettuple = fiting_gettuple,
		.amgetbitmap = NULL,
		.amendscan = fiting_endscan,
		.ammarkpos = NULL,
		.amrestrpos = NULL,
		.amestimateparallelscan = NULL,
		.aminitparallelscan = NULL,
		.amparallelrescan = NULL,
		.amtranslatestrategy = NULL,
		.amtranslatecmptype = NULL,
	};

	PG_RETURN_POINTER(&amroutine);
}

/*
 * fiting_validate — minimal opclass validation.
 * Returns true unconditionally for checkpoint 1.
 */
bool
fiting_validate(Oid opclassoid)
{
	return true;
}
