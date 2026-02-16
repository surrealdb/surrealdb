/**
[test]
reason = "Test EXPLAIN ANALYZE FORMAT JSON - JSON output with metrics"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "{ children: [{ attributes: { direction: 'Forward', table: 'item' }, context: 'Db', metrics: { output_rows: 3 }, operator: 'TableScan' }], context: 'Db', metrics: { output_rows: 3 }, operator: 'Project', total_rows: 3 }"
*/

-- Setup test data
{
	CREATE item:1 SET name = 'Alpha', score = 10;
	CREATE item:2 SET name = 'Beta', score = 20;
	CREATE item:3 SET name = 'Gamma', score = 30;
	RETURN "OK";
};

-- EXPLAIN ANALYZE FORMAT JSON a basic full table scan
EXPLAIN ANALYZE FORMAT JSON SELECT * FROM item;
