/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Streaming executor plan: when only a SINGLE-column index covers the IN-list and ORDER BY is on an UNRELATED column with a small literal LIMIT, the planner cannot k-way-merge (no composite suffix) but must mark the UnionIndexScan with `downstream_topk = true` so eager per-sub-stream prefetch is skipped. Regression guard for Phase 1 of the OOM fix."

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
value = ''''SelectProject [ctx: Db] [projections: created_at, http_status]\n    Limit [ctx: Db] [limit: 3]\n        SortTopKByKey [ctx: Db] [sort_keys: created_at DESC, limit: 3]\n            Filter [ctx: Db] [predicate: http_status INSIDE [200, 201]]\n                UnionIndexScan [ctx: Db] [table: record, branches: 2, downstream_topk: true]\n                    IndexScan [ctx: Db] [index: idx_status, access: = 200, direction: Forward]\n                    IndexScan [ctx: Db] [index: idx_status, access: = 201, direction: Forward]\n''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: created_at, http_status]\n    Limit [ctx: Db] [limit: 3]\n        SortTopKByKey [ctx: Db] [sort_keys: created_at ASC, limit: 3]\n            Filter [ctx: Db] [predicate: http_status INSIDE [200, 201]]\n                UnionIndexScan [ctx: Db] [table: record, branches: 2, downstream_topk: true]\n                    IndexScan [ctx: Db] [index: idx_status, access: = 200, direction: Forward]\n                    IndexScan [ctx: Db] [index: idx_status, access: = 201, direction: Forward]\n''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: created_at, http_status]\n    Limit [ctx: Db] [limit: 3, offset: 2]\n        SortTopKByKey [ctx: Db] [sort_keys: created_at DESC, limit: 5]\n            Filter [ctx: Db] [predicate: http_status INSIDE [200, 201]]\n                UnionIndexScan [ctx: Db] [table: record, branches: 2, downstream_topk: true]\n                    IndexScan [ctx: Db] [index: idx_status, access: = 200, direction: Forward]\n                    IndexScan [ctx: Db] [index: idx_status, access: = 201, direction: Forward]\n''''
*/

-- Single-column index on the IN-list column only.  ORDER BY is on
-- `created_at`, which is NOT in any index — the merge-by-index-key path
-- cannot engage, so the planner falls back to the downstream_topk flag.
DEFINE INDEX idx_status ON record FIELDS http_status;

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

-- DESC: UnionIndexScan with downstream_topk = true, no merge mode.
EXPLAIN SELECT created_at, http_status FROM record
	WHERE http_status IN [200, 201]
	ORDER BY created_at DESC
	LIMIT 3;

-- ASC mirror.
EXPLAIN SELECT created_at, http_status FROM record
	WHERE http_status IN [200, 201]
	ORDER BY created_at ASC
	LIMIT 3;

-- START + LIMIT still within the priority-queue threshold.
EXPLAIN SELECT created_at, http_status FROM record
	WHERE http_status IN [200, 201]
	ORDER BY created_at DESC
	START 2 LIMIT 3;
