/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro", "best-effort-ro"]
timeout = 15000

[test]
reason = """
	A top-k scan skips record decode once the sort's heap is full. The rows
	arrive best first, so after the first batch every later row ties or trails
	the heap's worst entry, and the scan beneath the sort reads that threshold
	and rejects them undecoded. The scan emits only the batches it read before
	the sort published, far fewer than the table holds. A scan that found no
	threshold would emit every row and return the same answer, so the scan's
	row count is the only thing that shows the pushdown is armed.
"""

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.children[0].children[0].children[0].operator = 'TableScan' && $result.children[0].children[0].children[0].metrics.output_rows < 8000"

[[test.results]]
value = "[12000, 11999, 11998, 11997, 11996]"
*/

FOR $i IN 0..12000 { CREATE type::record('t', $i) SET n = 12000 - $i; };

EXPLAIN ANALYZE FORMAT JSON SELECT * FROM t ORDER BY n DESC LIMIT 5;
SELECT VALUE n FROM t ORDER BY n DESC LIMIT 5;
