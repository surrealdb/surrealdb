/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A write's `ONLY` makes its value the one record it returned, and a
	`TIMEOUT` beside it bounds the work without changing that shape: every
	write statement answers a bare record, at the top level and as a subquery.
	A write without `ONLY` still answers its rows under a `TIMEOUT`.
"""

[[test.results]]
value = "{ a: 1, id: person:1 }"

[[test.results]]
value = "{ a: 1, b: 1, id: person:1 }"

[[test.results]]
value = "{ b: 1, id: person:2 }"

[[test.results]]
value = "{ a: 1, b: 1, id: person:1 }"

[[test.results]]
value = "{ id: edge:1, in: person:2, num: 1, out: person:3 }"

[[test.results]]
value = "{ id: person:4 }"

[[test.results]]
value = "[{ id: person:5 }]"

*/
CREATE ONLY person:1 CONTENT { a: 1 } TIMEOUT 30s;
UPDATE ONLY person:1 MERGE { b: 1 } TIMEOUT 30s;
UPSERT ONLY person:2 MERGE { b: 1 } TIMEOUT 30s;
DELETE ONLY person:1 RETURN BEFORE TIMEOUT 30s;
RELATE ONLY person:2->edge:1->person:3 CONTENT { num: 1 } TIMEOUT 30s;
RETURN (CREATE ONLY person:4 TIMEOUT 30s);
CREATE person:5 TIMEOUT 30s;
