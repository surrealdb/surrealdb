/**
[env]
namespace = true
database = true

[test]
reason = "Plan: when only a SINGLE-column index covers the IN-list and ORDER BY is on an UNRELATED column with a small literal LIMIT, the union cannot be k-way-merged (no composite suffix), so its rows reach a bounded Sort (`keep: N`) that discards most of them. The union's branches are read on demand by the program, so no branch buffers ahead of the Sort and the plan carries no merge mode. Guard against the Sort being dropped or the merge being claimed."

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
value = '''
'SelectProject [ctx: Db] [projections: created_at, http_status]
    Limit [ctx: Db] [limit: 3]
        Sort [ctx: Db] [sort_keys: created_at DESC, keep: 3]
            Filter [ctx: Db] [predicate: http_status INSIDE [200, 201]]
                UnionIndexScan [ctx: Db] [table: record, branches: 2]
                    IndexScan [ctx: Db] [table: record, index: idx_status, access: = 200, direction: Forward]
                    IndexScan [ctx: Db] [table: record, index: idx_status, access: = 201, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: created_at, http_status]
    Limit [ctx: Db] [limit: 3]
        Sort [ctx: Db] [sort_keys: created_at ASC, keep: 3]
            Filter [ctx: Db] [predicate: http_status INSIDE [200, 201]]
                UnionIndexScan [ctx: Db] [table: record, branches: 2]
                    IndexScan [ctx: Db] [table: record, index: idx_status, access: = 200, direction: Forward]
                    IndexScan [ctx: Db] [table: record, index: idx_status, access: = 201, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: created_at, http_status]
    Limit [ctx: Db] [limit: 3, offset: 2]
        Sort [ctx: Db] [sort_keys: created_at DESC, keep: 5]
            Filter [ctx: Db] [predicate: http_status INSIDE [200, 201]]
                UnionIndexScan [ctx: Db] [table: record, branches: 2]
                    IndexScan [ctx: Db] [table: record, index: idx_status, access: = 200, direction: Forward]
                    IndexScan [ctx: Db] [table: record, index: idx_status, access: = 201, direction: Forward]
''''
*/

-- Single-column index on the IN-list column only.  ORDER BY is on
-- `created_at`, which is NOT in any index — the merge-by-index-key path
-- cannot engage, so the Sort keeps the top rows of the unmerged union.
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

-- DESC: UnionIndexScan with no merge mode under a bounded Sort.
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
