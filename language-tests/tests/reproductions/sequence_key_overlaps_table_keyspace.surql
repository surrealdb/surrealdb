/**
[env]
namespace = false
database = false
clean = true

[test]
reason = """
DEFINE SEQUENCE definitions used to be stored at /*{ns}*{db}*sq{name}, the only
database-level catalog key using the `*` sigil instead of `!`. At that byte
position `*` introduces a table name, so a sequence named `foo` encoded
identically to the keyspace root of a table named `sqfoo`, and the whole
keyspace of any table whose name begins with `sq` sorted inside the definition
scan range [/*{ns}*{db}*sq\\x00, /*{ns}*{db}*sr).

Listing sequences therefore decoded table-band values as SequenceDefinition and
failed, which broke sequence listing, INFO FOR DB, export, and -- because
removing a database lists its sequences to evict their cached state -- made the
database and its namespace impossible to drop.

Definitions now live at /*{ns}*{db}!sd{name}, a subspace holding nothing else.
"""

[[test.results]]
value = "{ database: 'pdb', namespace: 'pns' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: sqfoo:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "{  }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ foo: 'DEFINE SEQUENCE foo BATCH 10 START 0' }"

[[test.results]]
value = "0"

[[test.results]]
value = "NONE"

*/

USE NS pns DB pdb;

-- `sqfoo` is an ordinary table name. Its keyspace root is /*{ns}*{db}*sqfoo\0,
-- which is byte-identical to the definition key of a sequence named `foo`.
DEFINE TABLE sqfoo SCHEMAFULL;
DEFINE FIELD a ON sqfoo TYPE int;
CREATE sqfoo:1 SET a = 1;

-- No sequence exists yet, so both listings must be empty. Instead the field
-- key /*{ns}*{db}*sqfoo\0!fda\0 and the record key /*{ns}*{db}*sqfoo\0*<id>
-- fall inside the scan range and are decoded as SequenceDefinition.
INFO FOR DB STRUCTURE.sequences;
INFO FOR DB.sequences;

-- A real sequence must be listed, and only it.
DEFINE SEQUENCE foo BATCH 10 START 0;
INFO FOR DB.sequences;

-- Allocation itself is unaffected: nextval resolves the definition with a
-- point read rather than a scan.
sequence::nextval('foo');

-- REMOVE DATABASE lists the database's sequences to evict their cached state,
-- so the collision leaves the database (and its namespace) undroppable.
REMOVE DATABASE pdb;
