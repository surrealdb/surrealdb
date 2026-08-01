/**
[env]
imports = ["upgrade/define/sequence_table_band_collision_import.surql"]

[test]
upgrade = true
# The upgrade suite hops through adjacent version pairs, and the collision is
# still present in every release below 3.3.0 — a hop that ends below it, such as
# 3.0.0 => 3.0.1, would be asserting the bug is fixed in a version that does not
# contain the migration. Only the hop that lands on 3.3.0 or later can.
version = ">=3.3.0"
reason = "A datastore written by a pre-3.3 version, holding a sequence definition in the `*sq` table band next to a table named `sqfoo`, is migrated on startup: listing sequences works again, allocation continues from the pre-upgrade watermark, and the colliding table's data is untouched"

[[test.results]]
match = '''$result.sequences.foo == "DEFINE SEQUENCE foo BATCH 50 START 0"'''

[[test.results]]
match = '''$result.tables.sqfoo != NONE'''

[[test.results]]
value = "[{ a: 1, id: sqfoo:1 }]"

[[test.results]]
match = '''type::is_int($result) && $result >= 3'''

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON sqfoo TYPE int PERMISSIONS FULL' }"

*/

-- Before the migration this errored: the scan that lists sequence definitions
-- covered `sqfoo`'s field and record keys and decoded them as definitions.
INFO FOR DB;

-- The same statement's table listing must still be intact.
INFO FOR DB;

-- The colliding table's data survived the migration byte for byte.
SELECT * FROM sqfoo;

-- Allocation continues from where the from-version left off. It consumed 0, 1
-- and 2, so the next value is at least 3 — the migration moved the definition
-- without touching the allocator state under `!sq{name}`.
sequence::nextval('foo');

-- The table's schema is intact too, which the field key under the shared prefix
-- would not be if the migration had deleted what it could not identify.
SELECT VALUE fields FROM ONLY INFO FOR TABLE sqfoo;
