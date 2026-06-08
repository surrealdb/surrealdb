/**
[env]
namespace = true
database = true

[test]
reason = "Composite-index k-way merge: `WHERE col IN [..] ORDER BY suffix_col LIMIT N` with a composite index on (col, suffix_col) must produce the same correct top-k as the heap path, and the UnionIndexScan should use merge_by_index_key (visible via EXPLAIN)."

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
value = "[{ created_at: 9, http_status: 200 }, { created_at: 8, http_status: 201 }, { created_at: 7, http_status: 200 }]"

[[test.results]]
value = "[{ created_at: 0, http_status: 200 }, { created_at: 1, http_status: 201 }, { created_at: 2, http_status: 200 }]"

[[test.results]]
value = "[{ created_at: 7, http_status: 200 }, { created_at: 4, http_status: 201 }, { created_at: 2, http_status: 200 }]"

[[test.results]]
value = "[{ created_at: 0 }, { created_at: 1 }, { created_at: 2 }, { created_at: 4 }, { created_at: 7 }, { created_at: 8 }, { created_at: 9 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ created_at: 0, http_status: 200 }, { created_at: 1, http_status: 201 }, { created_at: 2, http_status: 200 }, { created_at: 4, http_status: 201 }, { created_at: 5, http_status: 200 }, { created_at: 5, http_status: 201 }]"
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

-- DESC top-3
SELECT created_at, http_status FROM record
	WHERE http_status IN [200, 201]
	ORDER BY created_at DESC
	LIMIT 3;

-- ASC top-3
SELECT created_at, http_status FROM record
	WHERE http_status IN [200, 201]
	ORDER BY created_at ASC
	LIMIT 3;

-- Same record values but with START offset
SELECT created_at, http_status FROM record
	WHERE http_status IN [200, 201]
	ORDER BY created_at DESC
	START 2 LIMIT 3;

-- Full result set (no LIMIT) must still come back ordered correctly.
SELECT created_at FROM record
	WHERE http_status IN [200, 201]
	ORDER BY created_at ASC;

-- Two records with the same created_at value still work — merge is
-- stable per branch, and equal keys break ties in branch order.
CREATE record:10 SET http_status = 200, created_at = 5 RETURN NONE;
CREATE record:11 SET http_status = 201, created_at = 5 RETURN NONE;

SELECT created_at, http_status FROM record
	WHERE http_status IN [200, 201]
	ORDER BY created_at ASC
	LIMIT 6;
