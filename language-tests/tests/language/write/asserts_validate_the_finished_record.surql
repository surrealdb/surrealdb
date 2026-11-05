/**
[env]
namespace = true
database = true

[test]
reason = """
An `ASSERT` clause validates a value rather than producing one, so it runs after
every field on the record has been produced. Each one therefore reads the record
as it will be stored: siblings hold their final values, and two fields asserting
against each other see one consistent record rather than each other's half-built
state.

`p1` and `p2` are the same schema with the fields renamed, and the rename is the
whole point: name order puts the reader after its dependency in one and before it
in the other, so both passing is what shows validation does not depend on
production order.

A `COMPUTED` field read by an `ASSERT` is materialised for the same pass, so
`p3` validates against the derived value; nothing else reads it, and it is still
not stored.

Also pinned: an `ASSERT` naming its own field still validates that field, mutual
asserts still reject the record they are there to reject, and `$value`, `$input`
and `$before` are still bound in the deferred clause.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a_src: 10, b_chk: 1, id: p1:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a_chk: 1, id: p2:1, z_src: 10 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a_chk: 1, age: 18, id: p3:ok, z_comp: true }]"

[[test.results]]
error = "Found 1 for field `a_chk`, with record `p3:no`, but field must conform to: z_comp = true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: p4:ok, n: 5 }]"

[[test.results]]
error = "Found -1 for field `n`, with record `p4:no`, but field must conform to: n > 0"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: usr:1 }]"

[[test.results]]
value = "[{ id: usr:2 }]"

[[test.results]]
value = "[{ id: fol:x, in: usr:1, out: usr:2 }]"

[[test.results]]
error = "Found usr:1 for field `in`, with record `fol:y`, but field must conform to: in != out"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: p5:1, v: 'HELLO' }]"

[[test.results]]
error = "Found 'BAD' for field `v`, with record `p5:2`, but field must conform to: $value = string::uppercase($input ?? 'x') AND $input != 'bad'"

[[test.results]]
value = "[{ id: p5:1, v: 'AGAIN' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: p6:1, n: 1 }]"

[[test.results]]
value = "[{ id: p6:1, n: 2 }]"

[[test.results]]
error = "Found 1 for field `n`, with record `p6:1`, but field must conform to: $before = NONE OR $value > $before"
*/

-- The dependency sorts before the reader in one table and after it in the other.
DEFINE TABLE p1 SCHEMALESS;
DEFINE FIELD a_src ON p1 TYPE option<number> VALUE 10;
DEFINE FIELD b_chk ON p1 TYPE option<number> DEFAULT 1 ASSERT a_src = 10;
CREATE p1:1;
DEFINE TABLE p2 SCHEMALESS;
DEFINE FIELD z_src ON p2 TYPE option<number> VALUE 10;
DEFINE FIELD a_chk ON p2 TYPE option<number> DEFAULT 1 ASSERT z_src = 10;
CREATE p2:1;

-- An ASSERT reading a COMPUTED field sees the derived value.
DEFINE TABLE p3 SCHEMALESS;
DEFINE FIELD age    ON p3 TYPE number;
DEFINE FIELD z_comp ON p3 COMPUTED age >= 18;
DEFINE FIELD a_chk  ON p3 TYPE option<number> DEFAULT 1 ASSERT z_comp = true;
CREATE p3:ok SET age = 18;
CREATE p3:no SET age = 17;

-- An ASSERT naming its own field validates that field, and is not a cycle.
DEFINE TABLE p4 SCHEMALESS;
DEFINE FIELD n ON p4 TYPE number ASSERT n > 0;
CREATE p4:ok SET n = 5;
CREATE p4:no SET n = -1;

-- Mutual asserts across a relation's endpoints.
DEFINE TABLE usr SCHEMALESS;
DEFINE TABLE fol SCHEMAFULL TYPE RELATION IN usr OUT usr;
DEFINE FIELD OVERWRITE in  ON fol TYPE record<usr> ASSERT in != out;
DEFINE FIELD OVERWRITE out ON fol TYPE record<usr> ASSERT out != in;
CREATE usr:1;
CREATE usr:2;
RELATE usr:1->fol:x->usr:2;
RELATE usr:1->fol:y->usr:1;

-- `$value`, `$input` and `$before` in a deferred ASSERT.
DEFINE TABLE p5 SCHEMALESS;
DEFINE FIELD v ON p5 TYPE option<string>
	VALUE string::uppercase($input ?? "x")
	ASSERT $value = string::uppercase($input ?? "x") AND $input != "bad";
CREATE p5:1 SET v = "hello";
CREATE p5:2 SET v = "bad";
UPDATE p5:1 SET v = "again";
DEFINE TABLE p6 SCHEMALESS;
DEFINE FIELD n ON p6 TYPE number ASSERT $before = NONE OR $value > $before;
CREATE p6:1 SET n = 1;
UPDATE p6:1 SET n = 2;
UPDATE p6:1 SET n = 1;
