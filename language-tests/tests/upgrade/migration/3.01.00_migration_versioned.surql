/**
[env]
imports = ["upgrade/migration/3.01.00_migration_versioned_import.surql"]
versioned = true

[test]
upgrade = true
migration = true
introduced = ">=3.1.0"
reason = """
A versioned RocksDB store is migrated with the versions it holds, so every
record is readable as it stood at the timestamp taken between the writes: the
one updated since, the one deleted since and the one never touched. A read before
the first write finds no table, as the catalog is read at that time too. A write
made after the migration is the newest version, and the old one stays readable
below it; a version converted to a time in the future would hide it."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: vh:changed, v: 1 }, { id: vh:deleted, v: 1 }, { id: vh:kept, v: 1 }]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[{ id: vh:changed, v: 2 }, { id: vh:kept, v: 1 }]"

[[test.results]]
error = "The table 'vh' does not exist"

[[test.results]]
value = "[{ id: vh:kept, v: 3 }]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[1]"

*/
LET $at = vh_marker:before.at;
SELECT id, v FROM vh ORDER BY id VERSION $at;
SELECT VALUE v FROM vh:deleted VERSION $at;
SELECT id, v FROM vh ORDER BY id;
SELECT id, v FROM vh ORDER BY id VERSION d'2000-01-01T00:00:00Z';
UPDATE vh:kept SET v = 3 RETURN id, v;
SELECT VALUE v FROM vh:kept;
SELECT VALUE v FROM vh:kept VERSION $at;
