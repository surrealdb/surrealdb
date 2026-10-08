/**
[env]

[test]
reason = "A mock target is never expanded into one subject per id: a bulk `UPSERT |bulk:500|` writes all 500 records and a count afterwards sees them. `mock_lazy_iteration_streaming.surql` pins the plan. CREATE has no EXPLAIN grammar, so this uses UPSERT without a condition (deferable, same code path as CREATE)."

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 500 }]"

*/
UPSERT |bulk:500| SET n = 1 RETURN NONE;
SELECT count() FROM bulk GROUP ALL;
