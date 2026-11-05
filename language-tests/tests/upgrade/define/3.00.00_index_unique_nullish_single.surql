/**
[env]
imports = ["upgrade/define/3.00.00_index_unique_nullish_single_import.surql"]

[test]
upgrade = true
# Releases before 3.1.0 write no entry for a NULL or NONE value on a UNIQUE
# index, so no reader finds one through the index.
importing-version = ">=3.1.0"
reason = """
A single-column UNIQUE index stores a NULL or NONE value as a non-unique entry
naming its record, so several coexist beside the unique slots of complete
values. An equality read on NULL or NONE returns every record the old writer
stored under it, and a complete value still collides."""

[[test.results]]
value = "[solo:2, solo:3]"

[[test.results]]
value = "[solo:5]"

[[test.results]]
value = "[solo:1]"

[[test.results]]
error = "Database index `solo_v` already contains 'a', with record `solo:1`"

[[test.results]]
value = "[{ id: solo:7, v: NULL }]"

[[test.results]]
value = "[solo:2, solo:3, solo:7]"

*/
SELECT VALUE id FROM solo WHERE v = NULL ORDER BY id;
SELECT VALUE id FROM solo WHERE v = NONE ORDER BY id;
SELECT VALUE id FROM solo WHERE v = 'a';
-- A complete value still collides; a NULL does not.
CREATE solo:6 SET v = 'a';
CREATE solo:7 SET v = NULL;
SELECT VALUE id FROM solo WHERE v = NULL ORDER BY id;
