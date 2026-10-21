/**
[env]
imports = ["language/planner/record_id_list_reads_listed_records_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
	A record user's `id IN <list>` reads the listed records by key and sees
	only those the table's select permission admits, with the fields its
	field permissions admit: the records Bob owns are no rows for Alice,
	whatever the list names, and the count agrees. A table that is not
	defined reads as empty for her, with or without a LIMIT, as a scan does.
"""

[[test.results]]
value = "[{ id: doc:1, n: 1, secret: 'a1' }, { id: doc:3, n: 3, secret: 'a3' }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

SELECT id, n, secret FROM doc WHERE id IN [doc:4, doc:3, doc:2, doc:1];
SELECT count() FROM doc WHERE id IN [doc:2, doc:3, doc:4] GROUP ALL;
SELECT VALUE id FROM doc WHERE id IN [doc:2, doc:4];
SELECT * FROM absent WHERE id IN [absent:1, absent:2];
SELECT * FROM absent WHERE id IN [absent:1, absent:2] LIMIT 0;
