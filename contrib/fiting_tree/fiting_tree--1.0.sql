/* contrib/fiting_tree/fiting_tree--1.0.sql */

-- Prevent loading this file directly from psql.
\echo Use "CREATE EXTENSION fiting_tree" to load this file. \quit

-- -------------------------------------------------------------------------
-- 1. Handler function
--    Returns the IndexAmRoutine that describes all FITing-Tree callbacks.
-- -------------------------------------------------------------------------
CREATE FUNCTION fiting_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- -------------------------------------------------------------------------
-- 2. Access method registration
-- -------------------------------------------------------------------------
CREATE ACCESS METHOD fiting
    TYPE INDEX
    HANDLER fiting_handler;

COMMENT ON ACCESS METHOD fiting IS
    'FITing-Tree: piecewise-linear learned index (CS349 Checkpoint 1)';

-- -------------------------------------------------------------------------
-- 3. Operator class for int4
--
--    Strategy 1  =   equality
--    Support  1  btint4cmp   comparison function (reused from btree)
--
--    Checkpoint 1 supports equality lookups only.
-- -------------------------------------------------------------------------
CREATE OPERATOR CLASS fiting_int4_ops
    DEFAULT FOR TYPE int4
    USING fiting AS
        OPERATOR 1  =  (int4, int4),
        FUNCTION 1  btint4cmp(int4, int4);
