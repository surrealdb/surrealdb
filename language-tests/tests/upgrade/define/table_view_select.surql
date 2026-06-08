/**
[env]
imports = ["upgrade/define/table_view_select_import.surql"]

[test]
upgrade = true
reason = "SELECT-projection view survives upgrade — pre-populated rows remain queryable AND new inserts into the source still propagate"

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
