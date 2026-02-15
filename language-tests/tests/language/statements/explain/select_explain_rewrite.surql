/**
[env]
new-planner-strategy = "all-ro"

[test]
reason = "Test that SELECT ... EXPLAIN rewrites to EXPLAIN SELECT (JSON format) and SELECT ... EXPLAIN FULL rewrites to EXPLAIN ANALYZE SELECT (JSON format) when using the new planner"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "{ children: [{ attributes: { direction: 'Forward', table: 'item' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Project' }"

[[test.results]]
value = "{ children: [{ attributes: { direction: 'Forward', predicate: 'score > 15', table: 'item' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Project' }"

[[test.results]]
value = "{ children: [{ attributes: { direction: 'Forward', table: 'item' }, context: 'Db', metrics: { output_rows: 3 }, operator: 'TableScan' }], context: 'Db', metrics: { output_rows: 3 }, operator: 'Project', total_rows: 3 }"

[[test.results]]
value = "{ children: [{ attributes: { direction: 'Forward', predicate: 'score > 15', table: 'item' }, context: 'Db', metrics: { output_rows: 2 }, operator: 'TableScan' }], context: 'Db', metrics: { output_rows: 2 }, operator: 'Project', total_rows: 2 }"

*/

-- Setup test data
{
	CREATE item:1 SET name = 'Alpha', score = 10;
	CREATE item:2 SET name = 'Beta', score = 20;
	CREATE item:3 SET name = 'Gamma', score = 30;
	RETURN "OK";
};

-- SELECT ... EXPLAIN rewrites to EXPLAIN FORMAT JSON SELECT ...
SELECT * FROM item EXPLAIN;

-- SELECT ... EXPLAIN with predicate
SELECT * FROM item WHERE score > 15 EXPLAIN;

-- SELECT ... EXPLAIN FULL rewrites to EXPLAIN ANALYZE FORMAT JSON SELECT ...
SELECT * FROM item EXPLAIN FULL;

-- SELECT ... EXPLAIN FULL with predicate
SELECT * FROM item WHERE score > 15 EXPLAIN FULL;
