/**
[env]
imports = ["upgrade/define/3.00.00_index_btree_identity_entry_ids_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
Records whose ids differ only in a nested number's kind (`ent:[1]` and
`ent:[1dec]`) are two records. A non-unique B-tree entry an older binary wrote
spells the record id without that kind, so the pair shares one entry and an
indexed read answers one of the two where the same read without the index
answers both. An index built on this checkout gives each record its own entry.
The id kinds are asserted with `type::is_int`, since the runner compares record
ids across number kinds.

The upgrade target is bounded to the current checkout: every released binary
shares the entry, rebuilt or not."""

[[test.repair]]
since = ">=3.4.0-a"
script = "upgrade/define/3.00.00_index_btree_identity_entry_ids_rebuild.surql"
unrepaired = "degraded"
reason = """
A non-unique B-tree index an older binary built holds one entry for records
whose ids differ only in a nested number's kind, so an indexed read returns one
of them. `REBUILD INDEX` rewrites the entries with each record's id kept whole."""

[[test.results]]
value = "[{ int: false }, { int: true }]"

[[test.results]]
value = "[{ int: false }, { int: true }]"

[[test.results]]
value = "[{ count: 2 }]"

*/
SELECT type::is_int(id[0]) AS int FROM ent WITH INDEX ent_body WHERE body = 'hello' ORDER BY int;
SELECT type::is_int(id[0]) AS int FROM ent WITH NOINDEX WHERE body = 'hello' ORDER BY int;
SELECT count() FROM ent WITH INDEX ent_body WHERE body = 'hello' GROUP ALL;
