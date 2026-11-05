/**
[test]
reason = """
	`INSERT IGNORE` of a record that exists stores nothing and answers no row.
	Two `INSERT IGNORE`s of one new record whose transactions overlap both find
	it absent and both write it, so the second to commit loses with a retryable
	`TransactionConflict`; retried, it finds the record and ignores it. The
	overlap needs two concurrent requests, which the runner does not issue, so
	it is pinned by `it::concurrency::overlapping_insert_ignores_of_one_record_conflict_and_a_retry_ignores_it`
	in `surrealdb-core`. This file holds what the retry sees.
"""
issue = 5518

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:1]"

[env]
namespace = true
database = true
auth = { level = "owner" }

*/
DEFINE TABLE t;
INSERT IGNORE INTO t { id: 1 } RETURN id;
INSERT IGNORE INTO t { id: 1 } RETURN id;
INSERT IGNORE INTO t { id: 1 } RETURN id;
SELECT VALUE id FROM t;
