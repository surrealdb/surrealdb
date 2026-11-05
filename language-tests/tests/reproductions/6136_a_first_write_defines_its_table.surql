/**
[test]
reason = """
	A write to a table that is not defined defines it, in the writing
	transaction. Two such first writes whose transactions overlap both write the
	definition, so the second to commit loses with a retryable
	`TransactionConflict`; retried, it finds the table defined and writes its
	records. The overlap needs two concurrent requests, which the runner does
	not issue, so it is pinned by
	`it::concurrency::overlapping_first_writes_to_an_undefined_table_conflict_and_a_retry_succeeds`
	in `surrealdb-core`. The report's `Multiple key errors` text is TiKV's
	unmapped form of the same conflict. This file holds the definition a first
	write leaves and that a later write reuses.
"""
issue = 6136

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "6"

[[test.results]]
value = "['mytable']"

[env]
namespace = true
database = true
auth = { level = "owner" }

*/
CREATE |mytable:3| CONTENT { name: rand::string(7, 15) } RETURN NONE;
CREATE |mytable:3| CONTENT { name: rand::string(7, 15) } RETURN NONE;
count(SELECT * FROM mytable);
object::keys((INFO FOR DB).tables);
