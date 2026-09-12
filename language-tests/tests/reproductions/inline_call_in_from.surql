/**
[env]
namespace = true
database = true
backend = ["mem", "rocksdb", "surrealkv"]

[test]
reason = """
	A call to a user-defined function in `FROM` whose body is one expression
	is inlined, and the source is then classified as the expression: a record
	id is a record read that keeps every clause of the statement — `FOR
	UPDATE` locks the record it names and `LIMIT 0` discards the row without
	an error — and answers the same rows as the call did.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:one, value: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: item:one, value: 1 }]"

[[test.results]]
value = "[1]"
*/

DEFINE FUNCTION fn::rid() { RETURN item:one };
CREATE item:one SET value = 1;
SELECT * FROM fn::rid() LIMIT 0 FOR UPDATE;
SELECT * FROM fn::rid() FOR UPDATE;
SELECT VALUE value FROM fn::rid();
