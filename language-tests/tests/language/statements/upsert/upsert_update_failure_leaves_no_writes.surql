/**
[env]
namespace = true
database = true

[test]
requires = ["scripting"]
reason = """
When the update an UPSERT falls back to fails part-way, it must leave none of
its writes behind, even where the statement's error does not fail the
transaction: a `DEFINE API` handler turns it into a 500 response, and a script
can catch a rejected `surrealdb.query`, and either way the transaction goes on
to commit.

The update stores the record before its index entries, so an update that
conflicts on a unique index has already rewritten the record when the index
refuses it. Left in place, that write commits a second record holding a value
the unique index attributes to another record. Both paths into the update are
covered: an UPSERT on a table whose create attempt conflicts on another unique
index and so falls back to updating the conflicting record, and an UPSERT
naming an existing record, which updates it directly. So is an event that
throws after the update stored the record.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "'caught'"

[[test.results]]
value = "[{ id: person:1, one: 'A', two: 'X' }, { id: person:2, one: 'B', two: 'Y' }]"

[[test.results]]
value = "[person:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "500"

[[test.results]]
value = "[{ id: person:1, one: 'A', two: 'X' }, { id: person:2, one: 'B', two: 'Y' }]"

[[test.results]]
value = "[person:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "'caught'"

[[test.results]]
value = "[{ id: ev:1, v: 1 }]"

[[test.results]]
value = "['Y2']"

[[test.results]]
value = "[person:1]"
*/

DEFINE TABLE person SCHEMALESS;
DEFINE INDEX u_one ON person FIELDS one UNIQUE;
DEFINE INDEX u_two ON person FIELDS two UNIQUE;
UPSERT person:1 SET one = 'A', two = 'X' RETURN NONE;
UPSERT person:2 SET one = 'B', two = 'Y' RETURN NONE;

-- The create attempt conflicts on `one`, so it falls back to updating
-- person:2, whose new `two` conflicts with person:1
function() {
	try {
		await surrealdb.query(`UPSERT person SET one = 'B', two = 'X'`);
	} catch (e) {
		return 'caught';
	}
	return 'not caught';
};
SELECT id, one, two FROM person ORDER BY id;
SELECT VALUE id FROM person WHERE two = 'X';

-- Naming the existing record updates it directly
DEFINE API '/upsert' FOR post THEN (UPSERT person:2 SET two = 'X');
api::invoke('/upsert', { method: 'post' }).status;
SELECT id, one, two FROM person ORDER BY id;
SELECT VALUE id FROM person WHERE two = 'X';

-- An event that throws after the update stored the record
DEFINE TABLE ev SCHEMALESS;
DEFINE EVENT refuse ON ev WHEN $event = 'UPDATE' THEN { THROW 'refused' };
UPSERT ev:1 SET v = 1 RETURN NONE;
function() {
	try {
		await surrealdb.query(`UPSERT ev:1 SET v = 2`);
	} catch (e) {
		return 'caught';
	}
	return 'not caught';
};
SELECT id, v FROM ev;

-- No unique index entry of the failed updates survived
UPDATE person:2 SET two = 'Y2' RETURN VALUE two;
SELECT VALUE id FROM person WHERE two = 'X';
