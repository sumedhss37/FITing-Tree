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
    'FITing-Tree: piecewise-linear learned index (CS349 Checkpoint 2)';

-- -------------------------------------------------------------------------
-- 3. Operator classes
--
--    Strategy 1  =   equality
--    Support  1      comparison function (reused from btree)
--
--    Supported types: int4, timestamp, timestamptz.
--    Keys are stored internally as int64 for all types.
-- -------------------------------------------------------------------------

-- int4
CREATE OPERATOR CLASS fiting_int4_ops
    DEFAULT FOR TYPE int4
    USING fiting AS
        OPERATOR 1  =  (int4, int4),
        FUNCTION 1  btint4cmp(int4, int4);

-- timestamp (without time zone)
CREATE OPERATOR CLASS fiting_timestamp_ops
    DEFAULT FOR TYPE timestamp
    USING fiting AS
        OPERATOR 1  =  (timestamp, timestamp),
        FUNCTION 1  timestamp_cmp(timestamp, timestamp);

-- timestamptz (with time zone)
CREATE OPERATOR CLASS fiting_timestamptz_ops
    DEFAULT FOR TYPE timestamptz
    USING fiting AS
        OPERATOR 1  =  (timestamptz, timestamptz),
        FUNCTION 1  timestamptz_cmp(timestamptz, timestamptz);

-- -------------------------------------------------------------------------
-- 4. Monitoring / introspection functions
-- -------------------------------------------------------------------------

-- fiting_index_info(index regclass)
--
-- Returns one row per directory segment with live structural details:
--   seg_idx      int4   — 0-based segment position
--   seg_type     text   — 'fiting' or 'btree'
--   start_key    int8   — first key of the segment's range
--   total_tuples int4   — total (live + deleted) tuples in data pages
--   num_deleted  int4   — tombstone count (set by ambulkdelete)
--   data_pages   int4   — number of linked-list data page nodes
--   buf_tuples   int4   — tuples currently in the segment's buffer page
--
-- Example:
--   SELECT * FROM fiting_index_info('my_idx');
--
--   SELECT seg_type, count(*), sum(total_tuples)
--   FROM   fiting_index_info('my_idx')
--   GROUP BY seg_type;
CREATE FUNCTION fiting_index_info(index regclass)
RETURNS TABLE (
    seg_idx      int4,
    seg_type     text,
    start_key    int8,
    total_tuples int4,
    num_deleted  int4,
    data_pages   int4,
    buf_tuples   int4
)
AS 'MODULE_PATHNAME', 'fiting_index_info'
LANGUAGE C STRICT;

-- fiting_index_meta(index regclass)
--
-- Returns a single row of index-level metadata:
--   max_error       int4   — current max_error stored in the meta page
--   total_tuples    int8   — total live tuples across all segments
--   num_segments    int4   — number of directory segments
--   freelist_pages  int4   — pages currently on the freelist
--   tracked_pages   int4   — total pages under ref-count tracking
--
-- Example:
--   SELECT * FROM fiting_index_meta('my_idx');
CREATE FUNCTION fiting_index_meta(index regclass)
RETURNS TABLE (
    max_error      int4,
    total_tuples   int8,
    num_segments   int4,
    freelist_pages int4,
    tracked_pages  int4
)
AS 'MODULE_PATHNAME', 'fiting_index_meta'
LANGUAGE C STRICT;
