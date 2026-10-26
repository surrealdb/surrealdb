/**
[env]
namespace = true
database = true

[test]
reason = """
A plain INSERT (no IGNORE, no ON DUPLICATE KEY UPDATE) that fails on one row
fails the whole statement, and nothing the statement wrote survives: not the
rows before the failing one, not the failing row's record, its unique index
entries, its graph edges or its record references, and not the writes its
events made. This holds outside a transaction, where the statement's own
transaction is cancelled, and inside BEGIN/COMMIT, where the whole transaction
is aborted.

The unique index on `handle` is checked before the one on `email`, so the
failing row has already written its `handle` entry when `email` conflicts.
Re-inserting that `handle` afterwards proves the entry did not survive.
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ email: 'e0', handle: 'h0', id: person:0 }]"

[[test.results]]
error = "Database index `z_email` already contains 'e0', with record `person:0`"

[[test.results]]
value = "[{ email: 'e0', handle: 'h0', id: person:0 }]"

[[test.results]]
value = "[{ person: person:0 }]"

[[test.results]]
value = "[{ email: 'e9', handle: 'h2', id: person:9 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Database index `z_email` already contains 'e5', with record `person:5`"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[{ email: 'e0', handle: 'h0', id: person:0 }, { email: 'e9', handle: 'h2', id: person:9 }]"

[[test.results]]
value = "[{ person: person:0 }, { person: person:9 }]"

[[test.results]]
value = "[{ email: 'e5', handle: 'h5', id: person:5 }, { email: 'e6', handle: 'h6', id: person:6 }]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database index `pair` already contains [person:0, person:9], with record `knows:1`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event guard: An error occurred: no comment on person:9"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

DEFINE TABLE person SCHEMALESS;
DEFINE INDEX a_handle ON person FIELDS handle UNIQUE;
DEFINE INDEX z_email ON person FIELDS email UNIQUE;
DEFINE TABLE audit SCHEMALESS;
DEFINE EVENT log ON person WHEN $event = "CREATE" THEN (CREATE audit SET person = $after.id);
DEFINE TABLE knows TYPE RELATION IN person OUT person;
DEFINE INDEX pair ON knows FIELDS in, out UNIQUE;
DEFINE FIELD author ON comment TYPE option<record<person>> REFERENCE;

INSERT INTO person { id: 0, handle: 'h0', email: 'e0' };

-- Outside a transaction: row 2 conflicts on `email` after writing `handle`
INSERT INTO person [
	{ id: 1, handle: 'h1', email: 'e1' },
	{ id: 2, handle: 'h2', email: 'e0' },
	{ id: 3, handle: 'h3', email: 'e3' }
];
SELECT * FROM person ORDER BY id;
SELECT person FROM audit ORDER BY person;
-- The failing row's `handle` entry did not survive
INSERT INTO person { id: 9, handle: 'h2', email: 'e9' };

-- Inside a transaction: the whole transaction is aborted
BEGIN;
INSERT INTO person [
	{ id: 5, handle: 'h5', email: 'e5' },
	{ id: 6, handle: 'h6', email: 'e5' }
];
COMMIT;
SELECT * FROM person ORDER BY id;
SELECT person FROM audit ORDER BY person;
-- Nothing of the aborted transaction holds an index entry
INSERT INTO person [
	{ id: 5, handle: 'h5', email: 'e5' },
	{ id: 6, handle: 'h6', email: 'e6' }
] RETURN AFTER;
DELETE person:5, person:6;

-- INSERT RELATION: the failing edge is written before its unique index fails
INSERT RELATION INTO knows [
	{ id: 1, in: person:0, out: person:9 },
	{ id: 2, in: person:0, out: person:9 }
];
SELECT * FROM knows;
SELECT VALUE ->knows->person FROM ONLY person:0;
SELECT VALUE <-knows<-person FROM ONLY person:9;

-- Record references and event writes: the event writes, then fails, after
-- the reference is recorded
DEFINE EVENT guard ON comment WHEN $event = "CREATE" THEN {
	CREATE audit SET comment = $after.id;
	IF $after.author = person:9 { THROW "no comment on person:9" };
};
INSERT INTO comment [
	{ id: 1, author: person:0 },
	{ id: 2, author: person:9 }
];
SELECT * FROM comment;
SELECT VALUE <~comment FROM ONLY person:9;
SELECT VALUE comment FROM audit WHERE comment != NONE;
