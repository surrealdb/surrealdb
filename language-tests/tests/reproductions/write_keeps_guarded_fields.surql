/**
[env]
imports = ["reproductions/write_keeps_guarded_fields_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A write stores the record as it is stored, not as the caller may read it:
	a record user updating records it may not see a field of, whichever way
	the statement names them — a subquery yielding their ids, their rows or a
	projection of them, the table, or a record id — leaves that field in
	storage. The table's read permission holds only while the field keeps its
	stored value, so every record stays readable after each write.
"""

[[test.results]]
value = "[{ n: 11 }, { n: 12 }]"

[[test.results]]
value = "[{ id: doc:1, n: 11 }, { id: doc:2, n: 12 }]"

[[test.results]]
value = "[{ n: 21 }, { n: 22 }]"

[[test.results]]
value = "[{ id: doc:1, n: 21 }, { id: doc:2, n: 22 }]"

[[test.results]]
value = "[{ n: 31 }]"

[[test.results]]
value = "[{ id: doc:1, n: 31 }, { id: doc:2, n: 22 }]"

[[test.results]]
value = "[{ n: 41 }, { n: 32 }]"

[[test.results]]
value = "[{ id: doc:1, n: 41 }, { id: doc:2, n: 32 }]"

[[test.results]]
value = "[{ n: 51 }, { n: 42 }]"

[[test.results]]
value = "[{ id: doc:1, n: 51 }, { id: doc:2, n: 42 }]"

*/

UPDATE (SELECT VALUE id FROM doc) SET n += 10 RETURN n;
SELECT id, n FROM doc;
UPDATE doc SET n += 10 RETURN n;
SELECT id, n FROM doc;
UPDATE doc:1 SET n += 10 RETURN n;
SELECT id, n FROM doc;
UPDATE (SELECT * FROM doc) SET n += 10 RETURN n;
SELECT id, n FROM doc;
UPDATE (SELECT id, n FROM doc) SET n += 10 RETURN n;
SELECT id, n FROM doc;
