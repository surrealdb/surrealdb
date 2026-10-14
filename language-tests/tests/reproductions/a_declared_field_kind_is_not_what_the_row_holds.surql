/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A field's declared kind says what a write must satisfy, not what a stored
	row holds. Redefining the kind does not rewrite the rows already written,
	so a field declared `TYPE object` can hold a record id, and reading
	through it fetches that record and derives its `COMPUTED` fields — the
	effect a dereference has.

	That matters to the planner rather than to the caller. A rule tempted to
	answer "this step cannot dereference, so the projection may be evaluated
	a column at a time" from the source table's `field_kind` would be reading
	the declaration as a checked property of the value. It is not one, which
	is the same reason the `kind` rule refuses to drop a cast to a stored
	field's declared kind.

	The derived field is the evidence a claim about kinds cannot reach: `n` is
	a `COMPUTED` field of the linked record, so a read that yields it fetched
	the record, through a field the catalog says holds an object.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "linked:1"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ x: true }]"

*/

DEFINE TABLE linked SCHEMALESS;
DEFINE FIELD n ON linked COMPUTED rand::uuid();
CREATE linked:1 RETURN NONE;

DEFINE TABLE holder SCHEMAFULL;
DEFINE FIELD addr ON holder TYPE record<linked>;
CREATE holder:1 SET addr = linked:1 RETURN NONE;

-- Redeclare the kind. The row already written is not touched.
DEFINE FIELD OVERWRITE addr ON holder TYPE object;
RETURN (SELECT VALUE addr FROM holder:1)[0];
RETURN type::is_record((SELECT VALUE addr FROM holder:1)[0]);

-- A read through a field the catalog declares cannot hold a record.
SELECT type::is_uuid(addr.n) AS x FROM holder;
