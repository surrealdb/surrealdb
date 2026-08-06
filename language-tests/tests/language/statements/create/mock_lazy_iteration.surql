/**
[test]
reason = "A mock target (|table:N|) is carried as a single lazy iterable and drained id-by-id, not pre-expanded into one Iterable per id — asserted here via EXPLAIN, which reports exactly one 'Iterate Mock Defer' entry for the whole batch regardless of N. CREATE has no EXPLAIN grammar, so this uses UPSERT without a condition (deferable, same code path as CREATE)."

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 500 }]"

[[test.results]]
value = "[{ detail: { table: 'bulk' }, operation: 'Iterate Mock Defer' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"
*/

UPSERT |bulk:500| SET n = 1 RETURN NONE;
SELECT count() FROM bulk GROUP ALL;
UPSERT |bulk:1..=1| SET n = 1 RETURN NONE EXPLAIN;
