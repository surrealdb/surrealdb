/**
[env]
namespace = true
database = true

[test]
requires = ["scripting"]
reason = """
When the update an `ON DUPLICATE KEY UPDATE` falls back to fails part-way, it
must leave none of its writes behind, even where the statement's error does
not fail the transaction: a `DEFINE API` handler turns it into a 500 response,
and a script can catch a rejected `surrealdb.query`, and either way the
transaction goes on to commit.

The update stores the record before its index entries, so an update that
conflicts on a unique index has already rewritten the record when the index
refuses it. Left in place, that write commits a second record holding a value
the unique index attributes to another record. The same holds for an event that
throws after the record was stored, and for `INSERT RELATION`.

The rollback has to reach the transaction's cache too. An event that reads the
record before throwing caches the update's value, and one that defines a field
caches the definition; a later read or write in the same transaction must see
the record and the schema as the rollback restored them.

Each failing statement below names only the conflicting row, so what survives
is exactly what the failed update wrote. Whether rows a multi-row statement
wrote before a caught failure should survive — including when a statement
timeout fires after a row completed — is a statement-level question shared
with CREATE, UPDATE and RELATE, and is not what this test pins.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database index `u_email` already contains 'e1', with record `person:1`"

[[test.results]]
value = "[{ email: 'e1', id: person:1 }, { email: 'e2', id: person:2 }]"

[[test.results]]
value = "'caught'"

[[test.results]]
value = "[{ email: 'e1', id: person:1 }, { email: 'e2', id: person:2 }]"

[[test.results]]
value = "[person:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "500"

[[test.results]]
value = "[{ email: 'e1', id: person:1 }, { email: 'e2', id: person:2 }]"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "'caught'"

[[test.results]]
value = "[{ id: likes:1, tag: 't1' }, { id: likes:2, tag: 't2' }]"

[[test.results]]
value = "[likes:1]"

[[test.results]]
value = "['e2b']"

[[test.results]]
value = "['t2b']"

[[test.results]]
value = "[person:1]"

[[test.results]]
value = "[likes:1]"

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
value = "500"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

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
value = "500"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: target:real }]"

*/

DEFINE TABLE person SCHEMALESS;
DEFINE INDEX u_email ON person FIELDS email UNIQUE;
INSERT INTO person [{ id: 1, email: 'e1' }, { id: 2, email: 'e2' }] RETURN NONE;

-- Uncaught, the statement fails and its transaction with it
INSERT INTO person [{ id: 2, email: 'e2' }] ON DUPLICATE KEY UPDATE email = 'e1';
SELECT id, email FROM person ORDER BY id;

-- Caught by a script
function() {
	try {
		await surrealdb.query(`INSERT INTO person [{ id: 2, email: 'e2' }] ON DUPLICATE KEY UPDATE email = 'e1'`);
	} catch (e) {
		return 'caught';
	}
	return 'not caught';
};
SELECT id, email FROM person ORDER BY id;
SELECT VALUE id FROM person WHERE email = 'e1';

-- Caught by an API handler
DEFINE API '/odku' FOR post THEN (INSERT INTO person [{ id: 2, email: 'e2' }] ON DUPLICATE KEY UPDATE email = 'e1');
api::invoke('/odku', { method: 'post' }).status;
SELECT id, email FROM person ORDER BY id;

-- An event that throws after the update stored the record
DEFINE TABLE ev SCHEMALESS;
DEFINE EVENT refuse ON ev WHEN $event = 'UPDATE' THEN { THROW 'refused' };
INSERT INTO ev { id: 1, v: 1 } RETURN NONE;
function() {
	try {
		await surrealdb.query(`INSERT INTO ev { id: 1, v: 1 } ON DUPLICATE KEY UPDATE v = 2`);
	} catch (e) {
		return 'caught';
	}
	return 'not caught';
};
SELECT id, v FROM ev;

-- INSERT RELATION falls back to the same update
DEFINE TABLE likes TYPE RELATION SCHEMALESS;
DEFINE INDEX u_tag ON likes FIELDS tag UNIQUE;
INSERT RELATION INTO likes [
	{ id: 1, in: a:1, out: b:1, tag: 't1' },
	{ id: 2, in: a:2, out: b:2, tag: 't2' }
] RETURN NONE;
function() {
	try {
		await surrealdb.query(`INSERT RELATION INTO likes [{ id: 2, in: a:2, out: b:2, tag: 't2' }] ON DUPLICATE KEY UPDATE tag = 't1'`);
	} catch (e) {
		return 'caught';
	}
	return 'not caught';
};
SELECT id, tag FROM likes ORDER BY id;
SELECT VALUE id FROM likes WHERE tag = 't1';

-- No unique index entry of the failed updates survived: each record can still
-- take a fresh value, and the contested value still belongs to its owner alone
UPDATE person:2 SET email = 'e2b' RETURN VALUE email;
UPDATE likes:2 SET tag = 't2b' RETURN VALUE tag;
SELECT VALUE id FROM person WHERE email = 'e1';
SELECT VALUE id FROM likes WHERE tag = 't1';

-- The failed update's value does not outlive the rollback in the
-- transaction's record cache
DEFINE TABLE rc SCHEMALESS;
DEFINE EVENT peek_then_refuse ON rc WHEN $event = 'UPDATE' THEN { LET $seen = (SELECT VALUE v FROM ONLY $after.id); THROW 'refused' };
INSERT INTO rc { id: 1, v: 1 } RETURN NONE;
DEFINE API '/rc' FOR post THEN (INSERT INTO rc { id: 1, v: 1 } ON DUPLICATE KEY UPDATE v = 2);
BEGIN;
api::invoke('/rc', { method: 'post' }).status;
rc:1.v;
COMMIT;
rc:1.v;

-- A definition the failed update's event wrote does not outlive the rollback
-- in the transaction's catalog cache
DEFINE TABLE target SCHEMALESS;
DEFINE TABLE ddl SCHEMALESS;
DEFINE EVENT define_then_refuse ON ddl WHEN $event = 'UPDATE' THEN { DEFINE FIELD ghost ON target VALUE 99; CREATE target:seen; THROW 'refused' };
INSERT INTO ddl { id: 1, v: 1 } RETURN NONE;
DEFINE API '/ddl' FOR post THEN (INSERT INTO ddl { id: 1, v: 1 } ON DUPLICATE KEY UPDATE v = 2);
BEGIN;
api::invoke('/ddl', { method: 'post' }).status;
CREATE target:real RETURN VALUE ghost;
COMMIT;
SELECT * FROM target;
