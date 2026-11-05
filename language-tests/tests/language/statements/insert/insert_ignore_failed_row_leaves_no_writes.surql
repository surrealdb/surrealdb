/**
[env]
namespace = true
database = true

[test]
reason = """
INSERT IGNORE carries on past a row whose create attempt conflicts, and ON
DUPLICATE KEY UPDATE turns a conflicting create attempt into an update, so the
transaction commits with the failed attempt inside it. A create attempt whose
error a `DEFINE API` handler catches leaves the transaction to commit as well.
Each failed attempt must leave nothing behind: not its record, its unique index
entries, its graph edges, its record references or the writes its events made,
while the rows around it are kept.

The unique index on `handle` is checked before the one on `email`, so a
conflicting row has already written its `handle` entry when `email` conflicts.
Inserting that `handle` again afterwards proves the entry did not survive.
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
value = "NONE"

[[test.results]]
value = "[{ email: 'e0', handle: 'h0', hits: 0, id: person:0 }]"

[[test.results]]
value = "[{ email: 'e1', handle: 'h1', id: person:1 }, { email: 'e3', handle: 'h3', id: person:3 }]"

[[test.results]]
value = "[{ email: 'e0', handle: 'h0', hits: 0, id: person:0 }, { email: 'e1', handle: 'h1', id: person:1 }, { email: 'e3', handle: 'h3', id: person:3 }]"

[[test.results]]
value = "[{ person: person:0 }, { person: person:1 }, { person: person:3 }]"

[[test.results]]
value = "[{ email: 'e9', handle: 'h2', id: person:9 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ email: 'e0', handle: 'h0', hits: 1 }, { email: 'e1', handle: 'h1', hits: NONE }, { email: 'e3', handle: 'h3', hits: NONE }, { email: 'e5', handle: 'h5', hits: NONE }, { email: 'e9', handle: 'h2', hits: NONE }]"

[[test.results]]
value = "[person:0]"

[[test.results]]
value = "[person:10]"

[[test.results]]
value = "[{ id: knows:1, in: person:0, out: person:9 }, { id: knows:3, in: person:1, out: person:9 }]"

[[test.results]]
value = "[{ id: knows:1, in: person:0, out: person:9 }, { id: knows:3, in: person:1, out: person:9 }]"

[[test.results]]
value = "[person:9]"

[[test.results]]
value = "[person:0, person:1]"

[[test.results]]
value = "[{ author: person:0, id: comment:1 }, { author: person:1, id: comment:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "500"

[[test.results]]
value = "[{ author: person:0, id: comment:1 }, { author: person:1, id: comment:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[comment:1]"

[[test.results]]
value = "[comment:1, comment:3]"
*/

DEFINE TABLE person SCHEMALESS;
DEFINE INDEX a_handle ON person FIELDS handle UNIQUE;
DEFINE INDEX z_email ON person FIELDS email UNIQUE;
DEFINE TABLE audit SCHEMALESS;
DEFINE EVENT log ON person WHEN $event = "CREATE" THEN (CREATE audit SET person = $after.id);
DEFINE TABLE knows TYPE RELATION IN person OUT person;
DEFINE INDEX pair ON knows FIELDS in, out UNIQUE;
DEFINE FIELD author ON comment TYPE option<record<person>> REFERENCE;
DEFINE EVENT guard ON comment WHEN $event = "CREATE" THEN {
	CREATE audit SET comment = $after.id;
	IF $after.author = person:9 { THROW "no comment on person:9" };
};

INSERT INTO person { id: 0, handle: 'h0', email: 'e0', hits: 0 };

-- INSERT IGNORE: row 2 conflicts on `email` after writing `handle`
INSERT IGNORE INTO person [
	{ id: 1, handle: 'h1', email: 'e1' },
	{ id: 2, handle: 'h2', email: 'e0' },
	{ id: 3, handle: 'h3', email: 'e3' }
];
SELECT * FROM person ORDER BY id;
SELECT person FROM audit ORDER BY person;
-- The skipped row's `handle` entry did not survive
INSERT INTO person { id: 9, handle: 'h2', email: 'e9' };

-- ON DUPLICATE KEY UPDATE: the second row's create attempt writes `handle`
-- 'h4' under a generated id, conflicts on `email` and is retried as an update
-- of person:0
INSERT INTO person [
	{ handle: 'h5', email: 'e5' },
	{ handle: 'h4', email: 'e0' }
] ON DUPLICATE KEY UPDATE hits += 1 RETURN NONE;
SELECT handle, email, hits FROM person ORDER BY email;
SELECT VALUE person FROM audit WHERE person = person:0;
-- The abandoned create attempt's `handle` entry did not survive
INSERT INTO person { id: 10, handle: 'h4', email: 'e10' } RETURN VALUE id;

-- INSERT RELATION IGNORE: the failing edge is written before its unique index
-- fails
INSERT RELATION IGNORE INTO knows [
	{ id: 1, in: person:0, out: person:9 },
	{ id: 2, in: person:0, out: person:9 },
	{ id: 3, in: person:1, out: person:9 }
];
SELECT * FROM knows ORDER BY id;
SELECT VALUE ->knows->person FROM ONLY person:0;
SELECT VALUE <-knows<-person FROM ONLY person:9 ORDER BY id;

-- Record references and event writes: the event writes, then fails, after
-- the reference is recorded, and the handler catches the error
INSERT INTO comment [
	{ id: 1, author: person:0 },
	{ id: 3, author: person:1 }
];
DEFINE API '/comment' FOR post THEN (INSERT INTO comment { id: 2, author: person:9 } ON DUPLICATE KEY UPDATE author = person:9);
api::invoke('/comment', { method: 'post' }).status;
SELECT * FROM comment ORDER BY id;
SELECT VALUE <~comment FROM ONLY person:9;
SELECT VALUE <~comment FROM ONLY person:0;
SELECT VALUE comment FROM audit WHERE comment != NONE ORDER BY comment;
