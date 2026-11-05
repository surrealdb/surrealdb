/**
[env]

[test]
reason = "A write carries a mock target whole and drains its ids as it writes them. The plan names the mock itself as its `target` attribute and holds no compiled target expression at all, so what it carries is the same for every N — a mock is never expanded into one subject per id."

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 500 }]"

[[test.results]]
value = "{ attributes: { form: 'UPSERT' }, context: 'Db', operator: 'WriteOperator' }"
*/

UPSERT |bulk:500| SET n = 1 RETURN NONE;
SELECT count() FROM bulk GROUP ALL;
UPSERT |bulk:1..=1| SET n = 1 RETURN NONE EXPLAIN;
