/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A statement in a later transaction sees every definition committed before
	that transaction opened: those of the request's own earlier statements.
	Here ALTER and REMOVE of fields and tables take effect for the statements
	that follow them, and a removed table is no longer readable.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `a` of `t:1`: Expected `string` but found `1`"

[[test.results]]
value = "[{ a: 'x', b: 1, id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'b', but no such field exists for table 't'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 'z', id: t:4, v: 'set' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 'w', id: t:5 }]"

[[test.results]]
value = "[{ a: 'x', b: 1, id: t:2 }, { a: 'z', id: t:4, v: 'set' }, { a: 'w', id: t:5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 't' does not exist"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE FIELD a ON t TYPE int;
ALTER FIELD a ON t TYPE string;

CREATE t:1 SET a = 1;
CREATE t:2 SET a = 'x', b = 1;

ALTER TABLE t SCHEMAFULL;
CREATE t:3 SET a = 'y', b = 2;

DEFINE FIELD v ON t VALUE 'set';
CREATE t:4 SET a = 'z';

REMOVE FIELD v ON t;
CREATE t:5 SET a = 'w';

SELECT * FROM t ORDER BY id;
REMOVE TABLE t;
SELECT * FROM t;
