/**
[test]
reason = "Test EXPLAIN ANALYZE FORMAT JSON - JSON output with metrics"

[[test.results]]
value = "'OK'"

[[test.results]]
match = '''
$result.operator == "Project"
	AND $result.metrics.output_rows == 3
	AND $result.metrics.output_batches == 1
	AND $result.metrics.elapsed_ns > 0
	AND $result.total_rows == 3
	AND $result.children[0].operator == "Scan"
	AND $result.children[0].attributes.source == "item"
	AND $result.children[0].metrics.output_rows == 3
	AND $result.children[0].metrics.output_batches == 1
	AND $result.children[0].metrics.elapsed_ns > 0
'''
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
