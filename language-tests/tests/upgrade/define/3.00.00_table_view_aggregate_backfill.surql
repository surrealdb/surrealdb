/**
[env]
imports = ["upgrade/define/3.00.00_table_view_aggregate_backfill_import.surql"]

[test]
upgrade = true
reason = "An aggregating view that an old binary filled from rows that already existed is read by the new binary as the groups it holds, and the new binary maintains those rows as source rows are updated, created and deleted. The sibling 3.00.00_table_view_aggregate.surql defines its view before any row exists, so the rows an old binary writes when it fills a view are covered nowhere else."

[[test.results]]
value = "[{ id: sale_totals:['eu', 'x'], n: 2, product: 'x', region: 'eu', total: 30f }, { id: sale_totals:['eu', 'y'], n: 1, product: 'y', region: 'eu', total: 5f }, { id: sale_totals:['us', 'x'], n: 1, product: 'x', region: 'us', total: 7f }]"

[[test.results]]
value = "[{ id: sale_all:[], n: 4, total: 42f }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: sale_totals:['eu', 'x'], n: 1, product: 'x', region: 'eu', total: 20f }, { id: sale_totals:['eu', 'y'], n: 1, product: 'y', region: 'eu', total: 10f }, { id: sale_totals:['us', 'x'], n: 1, product: 'x', region: 'us', total: 7f }, { id: sale_totals:['us', 'y'], n: 1, product: 'y', region: 'us', total: 3f }]"

[[test.results]]
value = "[{ id: sale_all:[], n: 4, total: 40f }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: sale_totals:['eu', 'x'], n: 1, product: 'x', region: 'eu', total: 20f }, { id: sale_totals:['eu', 'y'], n: 1, product: 'y', region: 'eu', total: 10f }, { id: sale_totals:['us', 'y'], n: 1, product: 'y', region: 'us', total: 3f }]"

[[test.results]]
value = "[{ id: sale_all:[], n: 3, total: 33f }]"
*/
-- The groups the old binary stored when it filled the views.
SELECT * FROM sale_totals ORDER BY id;
SELECT * FROM sale_all;
-- A row changes group, one leaves, one arrives in a new group.
UPDATE sale:1 SET product = 'y' RETURN NONE;
DELETE sale:3 RETURN NONE;
CREATE sale:5 SET region = 'us', product = 'y', amount = 3 RETURN NONE;
SELECT * FROM sale_totals ORDER BY id;
SELECT * FROM sale_all;
-- The last row of a group leaves, and the group with it.
DELETE sale:4 RETURN NONE;
SELECT * FROM sale_totals ORDER BY id;
SELECT * FROM sale_all;
