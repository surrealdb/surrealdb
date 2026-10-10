/**
[env]
imports = ["upgrade/define/3.00.02_index_unique_nullish_coexist_import.surql"]

[test]
upgrade = true
introduced = ">=3.0.2"
# Releases 3.0.2 to 3.0.5 accept these tuples but write no entry for them, so
# no reader finds one through the index.
importing-version = ">=3.1.0"
reason = """
Tuples sharing a NONE or a NULL value coexist on a UNIQUE index as non-unique
entries, one per record, beside the unique slots of complete tuples. An
equality read on a nullish tuple returns every record the old writer stored
under it, a read pinning the leading value alone returns those and the
complete tuples together, and a complete tuple the old writer stored still
collides with an equal one written now."""

[[test.results]]
value = "[nul:2, nul:3]"

[[test.results]]
value = "[nul:4, nul:5]"

[[test.results]]
value = "[nul:1]"

[[test.results]]
value = "[nul:1, nul:2, nul:3, nul:4, nul:5]"

[[test.results]]
value = "[nul:6]"

[[test.results]]
error = "Database index `nul_ab` already contains ['k', 'v'], with record `nul:1`"

[[test.results]]
value = "[{ a: 'k', id: nul:7 }]"

[[test.results]]
value = "[nul:2, nul:3, nul:7]"

*/
SELECT VALUE id FROM nul WHERE a = 'k' AND b = NONE ORDER BY id;
SELECT VALUE id FROM nul WHERE a = 'k' AND b = NULL ORDER BY id;
SELECT VALUE id FROM nul WHERE a = 'k' AND b = 'v';
SELECT VALUE id FROM nul WHERE a = 'k' ORDER BY id;
SELECT VALUE id FROM nul WHERE a = 'j' ORDER BY id;
-- A complete tuple still collides; a nullish one does not.
CREATE nul:8 SET a = 'k', b = 'v';
CREATE nul:7 SET a = 'k';
SELECT VALUE id FROM nul WHERE a = 'k' AND b = NONE ORDER BY id;
