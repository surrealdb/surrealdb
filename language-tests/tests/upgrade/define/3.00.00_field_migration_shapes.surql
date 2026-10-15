/**
[env]
imports = ["upgrade/define/3.00.00_field_migration_shapes_import.surql"]

[test]
upgrade = true
reason = "Rows a released binary wrote, of nested, array, optional, record link and edge shapes, under indexes it built: each migration the current build runs over them reads back applied once, through the released binary's index and one built again, through a link and a graph hop, and after the database is raised to level 4, where a field removed over them is gone from every row."
# Migrations need the stored grammar of the current checkout.
version = ">=3.4.0-a"

[[test.results]]
error = "Cannot migrate field `qty` of table `doc`: the index `by_qty` reads it: remove the index, migrate, then define it again"

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
value = "[{ id: doc:1, meta: { note: '9', score: 1.5f }, opt: 10, qty: 5, tags: ['x', 'y'] }, { id: doc:2, meta: { score: -1f }, opt: NONE, qty: 0, tags: [] }, { id: doc:3, meta: { note: 'none', score: 3f }, opt: -1, qty: 7, tags: ['z'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 41, id: doc:1 }, { age: 0, id: doc:2 }, { age: NONE, id: doc:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[person:2], []]"

[[test.results]]
value = "[0, 2001]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, extra: { deep: [1, '2'] }, id: loose:1 }, { a: -1, id: loose:2, other: true }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: doc:1, meta: { note: 9, score: 1.5f }, name: 'a', owner: person:1, qty: 5, tags: ['x', 'y'] }, { id: doc:2, meta: { note: 0, score: -1f }, name: 'b', owner: person:2, qty: 0, tags: [] }, { id: doc:3, meta: { note: 0, score: 3f }, name: 'c', qty: 7, tags: ['z'] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: doc:1, qty: 5, tags: ['x', 'y'] }, { id: doc:2, qty: 1, tags: ['new'] }, { id: doc:3, qty: 7, tags: ['z'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change field `name` of table `doc`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[doc:3]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[doc:1, doc:3]"

[[test.results]]
value = "[doc:1, doc:3]"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { access: '= 7', direction: 'Forward', index: 'by_qty', table: 'doc' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = """{ attributes: { expr: 'id' }, children: [{ attributes: { access: "= 'b'", direction: 'Forward', index: 'by_name', table: 'doc' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'ProjectValue' }"""

# A removal step is named for the generation it follows, and a table a
# released binary defined holds fewer generations than one the current build
# defines when it replays the import itself outside an upgrade.
[[test.results]]
match = '''count($result) == 6
	&& array::slice($result, 0, 4) == ['qty_int', 'score_float', 'opt_int', 'note_int']
	&& string::starts_with($result[4], 'remove opt after ')
	&& string::starts_with($result[5], 'remove retired after ')'''
*/
-- The index the released binary built over `qty` reads it: the migration waits until the index goes.
ALTER FIELD qty ON doc TYPE int MIGRATE EXISTING ROWS AS qty_int USING <int>$value FALLBACK 0;
REMOVE INDEX by_qty ON doc;
-- The server runs the migration job beside the test, and a statement that
-- queues work for a table, or writes one of its rows, while the job writes that
-- table can lose a retryable conflict to it. Every such statement runs in this
-- transaction, so the job starts only once it has committed.
BEGIN;
ALTER FIELD qty ON doc TYPE int MIGRATE EXISTING ROWS AS qty_int USING <int>$value FALLBACK 0;
-- A nested field and an optional one migrate in every row; an absent value the transform keeps absent stays absent.
ALTER FIELD meta.score ON doc TYPE float MIGRATE EXISTING ROWS AS score_float USING <float>$value FALLBACK -1f;
ALTER FIELD opt ON doc TYPE option<int> MIGRATE EXISTING ROWS AS opt_int USING IF $value = NONE THEN NONE ELSE <int>$value END FALLBACK -1;
SELECT id, meta, opt, qty, tags FROM doc ORDER BY id;
-- A record link reads its target migrated.
ALTER FIELD age ON person TYPE int MIGRATE EXISTING ROWS AS age_int USING <int>$value FALLBACK 0;
SELECT id, owner.age AS age FROM doc ORDER BY id;
-- An edge's field migrates, and a graph hop filters on the migrated value.
ALTER FIELD since ON knows TYPE int MIGRATE EXISTING ROWS AS since_int USING <int>$value FALLBACK 0;
SELECT VALUE ->knows[WHERE since > 2000]->person FROM person ORDER BY id;
SELECT VALUE since FROM knows ORDER BY since;
-- A SCHEMALESS table keeps the fields it never declared.
ALTER FIELD a ON loose TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
SELECT * FROM loose ORDER BY id;
-- Raised to level 4, the enforced table takes a migration, and a field removed over the rows is gone from every row.
ALTER DATABASE COMPATIBILITY 4;
DEFINE FIELD OVERWRITE meta.note ON doc TYPE option<int> MIGRATE EXISTING ROWS AS note_int USING <int>$value FALLBACK 0;
REMOVE FIELD opt ON doc;
REMOVE FIELD retired ON doc;
SELECT * FROM doc ORDER BY id;
-- A write over a row behind every step reads each step applied once.
UPDATE doc:2 SET tags += 'new', qty += 1 RETURN NONE;
SELECT id, qty, tags FROM doc ORDER BY id;
COMMIT;
-- The enforced table refuses a retype its rows do not satisfy.
ALTER FIELD name ON doc TYPE int;
-- An index defined again over the rows, as the job rewrites them, holds their migrated values.
DEFINE INDEX by_qty ON doc FIELDS qty;
SELECT VALUE id FROM doc WHERE qty = 7;
SELECT VALUE id FROM doc WHERE qty = 1;
SELECT VALUE id FROM doc WHERE qty >= 5 ORDER BY id;
SELECT VALUE id FROM doc WITH NOINDEX WHERE qty >= 5 ORDER BY id;
SELECT VALUE id FROM doc WHERE qty = 7 EXPLAIN;
-- The index the released binary built over `name` still answers.
SELECT VALUE id FROM doc WHERE name = 'b';
SELECT VALUE id FROM doc WHERE name = 'b' EXPLAIN;
(INFO FOR TABLE doc).migrations.migrations.*.name;
