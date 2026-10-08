/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	Characterises a defect, not the intended behaviour. `UPSERT` attempts the
	create order first, which derives every `VALUE` clause of the table over the
	record the statement would create. A clause that reads a field the `SET` or
	`MERGE` does not write sees `NONE` there and fails, and the failure is not
	one of the schema failures the update retry recovers, so the statement fails
	although the record exists and the same write as an `UPDATE` succeeds. A
	`CONTENT` clause supplies the whole record and is unaffected. If this test
	starts failing, the create attempt has changed: confirm the new behaviour
	deliberately.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: h:1, n: 1, twice: 2, up: 5 }]"

[[test.results]]
value = "[{ id: h:1, n: 1, twice: 2, up: 1 }]"

[[test.results]]
error = "Cannot perform multiplication with 'none' and 'int'"

[[test.results]]
error = "Cannot perform multiplication with 'none' and 'int'"

[[test.results]]
value = "[{ id: h:1, n: 4, twice: 8, up: 3 }]"

[[test.results]]
value = "[{ id: h:1, n: 6, twice: 12, up: 7 }]"
*/

DEFINE TABLE h;
DEFINE FIELD n ON h TYPE option<int>;
DEFINE FIELD up ON h TYPE option<int>;
DEFINE FIELD twice ON h VALUE n * 2;
CREATE h:1 SET n = 1, up = 5;
UPDATE h:1 SET up = 1;
UPSERT h:1 SET up = 2;
UPSERT h:1 MERGE { up: 3 };
UPSERT h:1 CONTENT { n: 4, up: 3 };
UPSERT h:1 SET n = 6, up = 7;
