##Viewing/Updating Hyper Parameters

-- Monitor segment breakdown
SELECT seg_type, count(*), sum(total_tuples)
FROM fiting_index_info('my_idx') GROUP BY seg_type;

-- Check index health
SELECT * FROM fiting_index_meta('my_idx');

-- Tune and rebuild
ALTER INDEX my_idx SET (max_error = 64, btree_window_size = 8160);
REINDEX INDEX my_idx;