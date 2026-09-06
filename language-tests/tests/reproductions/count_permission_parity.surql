/**
[test]
reason = "count() must agree with SELECT about which rows a record identity can see. The count fast paths (CountScan and IndexCount) open-coded the permission check without `skip_fetch_perms` and without binding the document root, so a permission whose predicate reads another table saw that table's own permission applied and denied every row: SELECT returned 5 rows and count() returned 0 for the same identity, table and permission."

[[test.results]]
value = "5"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "0"

[[test.results]]
value = "0"

[env]
imports = ["reproductions/count_permission_parity_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

*/

-- The scan path. `grant`'s own permission denies this session, but inside a
-- permission predicate the read is bypassed, so all 5 rows are visible.
RETURN array::len((SELECT id FROM item));

-- CountScan: the unqualified count must agree with the scan above. This
-- returned 0 before the fix.
SELECT count() FROM item GROUP ALL;

-- IndexCount: the same, via the index on `n`.
SELECT count() FROM item WHERE n >= 0 GROUP ALL;

-- The WHERE predicate is not a permission and must still filter normally --
-- it must not be evaluated with permissions bypassed.
SELECT count() FROM item WHERE n < 2 GROUP ALL;

-- Denial must still deny: no grant row carries scope = 'secret', so this
-- identity sees nothing, and the count must not over-report either.
--
-- The count is read through an index expression rather than asserted as a
-- whole statement result: `GROUP ALL` over a fully filtered table yields
-- `[]` under the compute-only planner and `[{ count: 0 }]` under the
-- read-optimised ones. That shape difference is orthogonal to this fix --
-- the count is 0 either way -- and pinning it here would assert unrelated
-- behaviour.
RETURN array::len((SELECT id FROM secret));
RETURN (SELECT count() FROM secret GROUP ALL)[0].count ?? 0;
