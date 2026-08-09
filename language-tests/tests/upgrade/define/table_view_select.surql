/**
[env]
imports = ["upgrade/define/table_view_select_import.surql"]

[test]
upgrade = true
reason = "SELECT-projection view survives upgrade — pre-populated rows remain queryable AND new inserts into the source still propagate"
# 3.1.0 cannot read view rows written by an earlier version: it yields the
# source record id (`src:2`) where the view id (`high_scores:2`) is stored.
# Rows 3.1.0 writes itself read back correctly, and 3.1.1 onwards reads both
# older and 3.1.0-written stores correctly. The gate excludes 3.1.0 as an
# upgrade target so the rest of the hop still runs; it is not a WIP marker.
version = ">=3.1.1"

[[test.results]]
value = "[{ id: high_scores:2, name: 'b', score: 20 }, { id: high_scores:3, name: 'c', score: 30 }]"

[[test.results]]
value = "[{ id: src:4, name: 'd', score: 40 }]"

[[test.results]]
value = "[{ id: high_scores:2, name: 'b', score: 20 }, { id: high_scores:3, name: 'c', score: 30 }, { id: high_scores:4, name: 'd', score: 40 }]"

*/
SELECT * FROM high_scores ORDER BY id;
CREATE src:4 SET name = 'd', score = 40;
SELECT * FROM high_scores ORDER BY id;
