/**
[env]
namespace = true
database = true

[test]
reason = "ORDER BY a non-indexed column with IN-filter on an indexed column and a small LIMIT must return the correct top-k results without buffering all matching rows. Regression guard for the streaming UnionIndexScan + SortTopKByKey path."

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
*/

-- A composite index on (http_status, created_at) reproduces the crud-bench
-- shape: IN-filter on the prefix column plus ORDER BY on the suffix column.
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

-- DESC: the top 3 by created_at among rows with http_status IN [200,201]
-- are id 9, 8, 7. Excludes status 500 and 404.
SELECT created_at, http_status FROM record
	WHERE http_status IN [200, 201]
	ORDER BY created_at DESC
	LIMIT 3;

-- ASC mirror.
SELECT created_at, http_status FROM record
	WHERE http_status IN [200, 201]
	ORDER BY created_at ASC
	LIMIT 3;

-- START offset combined with LIMIT — pushes the heap threshold up.
SELECT created_at, http_status FROM record
	WHERE http_status IN [200, 201]
	ORDER BY created_at DESC
	START 2 LIMIT 3;
