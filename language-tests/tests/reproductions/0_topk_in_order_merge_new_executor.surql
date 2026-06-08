/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Streaming executor plan: with a composite index (filter_col, sort_col), `WHERE filter_col IN [...] ORDER BY sort_col` must route through UnionIndexScan with `merge_by_index_key` set. The merge produces globally sorted output so the planner eliminates the outer Sort and a downstream LIMIT cancels the scans early. Regression guard for Phase 2 of the OOM fix."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: created_at, http_status]\n    Limit [ctx: Db] [limit: 3]\n        Filter [ctx: Db] [predicate: http_status INSIDE [200, 201]]\n            UnionIndexScan [ctx: Db] [table: record, branches: 2, merge_by_index_key: created_at Desc]\n                IndexScan [ctx: Db] [index: idx_status_created, access: [200], direction: Backward]\n                IndexScan [ctx: Db] [index: idx_status_created, access: [201], direction: Backward]\n''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: created_at, http_status]\n    Limit [ctx: Db] [limit: 3]\n        Filter [ctx: Db] [predicate: http_status INSIDE [200, 201]]\n            UnionIndexScan [ctx: Db] [table: record, branches: 2, merge_by_index_key: created_at Asc]\n                IndexScan [ctx: Db] [index: idx_status_created, access: [200], direction: Forward]\n                IndexScan [ctx: Db] [index: idx_status_created, access: [201], direction: Forward]\n''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: created_at, http_status]\n    Limit [ctx: Db] [limit: 3, offset: 2]\n        Filter [ctx: Db] [predicate: http_status INSIDE [200, 201]]\n            UnionIndexScan [ctx: Db] [table: record, branches: 2, merge_by_index_key: created_at Desc]\n                IndexScan [ctx: Db] [index: idx_status_created, access: [200], direction: Backward]\n                IndexScan [ctx: Db] [index: idx_status_created, access: [201], direction: Backward]\n''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: created_at]\n    Filter [ctx: Db] [predicate: http_status INSIDE [200, 201]]\n        UnionIndexScan [ctx: Db] [table: record, branches: 2, merge_by_index_key: created_at Asc]\n            IndexScan [ctx: Db] [index: idx_status_created, access: [200], direction: Forward]\n            IndexScan [ctx: Db] [index: idx_status_created, access: [201], direction: Forward]\n''''
*/

DEFINE INDEX idx_status_created ON record FIELDS http_status, created_at;

CREATE record:0 SET http_status = 200, created_at = 0 RETURN NONE;
CREATE record:1 SET http_status = 201, created_at = 1 RETURN NONE;
CREATE record:2 SET http_status = 200, created_at = 2 RETURN NONE;
CREATE record:3 SET http_status = 500, created_at = 3 RETURN NONE;
CREATE record:4 SET http_status = 201, created_at = 4 RETURN NONE;
CREATE record:5 SET http_status = 404, created_at = 5 RETURN NONE;
CREATE record:6 SET http_status = 500, created_at = 6 RETURN NONE;
CREATE record:7 SET http_status = 200, created_at = 7 RETURN NONE;
CREATE record:8 SET http_status = 201, created_at = 8 RETURN NONE;
CREATE record:9 SET http_status = 200, created_at = 9 RETURN NONE;

-- DESC: UnionIndexScan should use merge_by_index_key: created_at Desc,
-- each branch should scan Backward, and the outer Sort should be
-- eliminated (no SortTopKByKey / Sort node in the plan).
EXPLAIN SELECT created_at, http_status FROM record
	WHERE http_status IN [200, 201]
	ORDER BY created_at DESC
	LIMIT 3;

-- ASC mirror — Forward branch scans.
EXPLAIN SELECT created_at, http_status FROM record
	WHERE http_status IN [200, 201]
	ORDER BY created_at ASC
	LIMIT 3;

-- START + LIMIT — both pushed through the merge.
EXPLAIN SELECT created_at, http_status FROM record
	WHERE http_status IN [200, 201]
	ORDER BY created_at DESC
	START 2 LIMIT 3;

-- No LIMIT — merge still produces a globally sorted stream covering
-- every matching record.
EXPLAIN SELECT created_at FROM record
	WHERE http_status IN [200, 201]
	ORDER BY created_at ASC;
