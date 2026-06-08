/**
[env]
imports = ["reproductions/codex_indexed_count_field_perm_leak_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:1" }

[test]
reason = "Indexed `SELECT count() ... GROUP ALL` over a WHERE clause that filters by a SELECT-denied field must not leak the count of matching values. The vulnerable behaviour was that the planner picked the index on `private` and used an index-only count fast path (`Iterate Index Count` in the legacy planner / `IndexCountScan` in the streaming planner) which never applies field-level SELECT permissions. The `WITH NOINDEX` equivalent correctly returns []; the indexed query must agree."

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"
*/

-- Baseline: a normal SELECT cannot use a denied field in WHERE — field
-- permissions reduce the document, so `private = "secret"` becomes
-- `NONE = "secret"` and the row is dropped.
SELECT public FROM data WHERE private = "secret";

-- Baseline: forcing no index, the document is loaded and permissions are
-- applied before the WHERE clause is re-evaluated, so the count is 0.
SELECT count() FROM data WITH NOINDEX WHERE private = "secret" GROUP ALL;

-- Vulnerability: the planner picks the index on `private` and uses an
-- index-only count fast path. Without the fix this returned `[{ count: 1 }]`
-- and leaked the cardinality of a value the record user cannot read.
SELECT count() FROM data WHERE private = "secret" GROUP ALL;

-- Sanity: counts over a field with FULL SELECT permissions must still use
-- the indexed fast path correctly. Both indexed and non-indexed forms must
-- agree — the fix must not over-trigger and break the optimisation when no
-- restricted field is referenced.
SELECT count() FROM data WITH NOINDEX WHERE public = "public" GROUP ALL;
SELECT count() FROM data WHERE public = "public" GROUP ALL;
