/**
[env]
namespace = true
database = true
auth = { level = "owner" }

planner-strategy = ["all-ro"]

[test]
reason = "Indexed `field = scalar` must agree with NOINDEX semantics when the field holds an array. An index on an array-valued column without [*]/FLATTEN still expands the array into per-element index entries, so a `tags = 'public'` lookup hits a row whose `tags = ['public', 'secret']`. The IndexScan must re-evaluate the WHERE clause on the fetched record so the array row is rejected, matching SurrealQL filter semantics (array == scalar is false)."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'array row', id: doc:leak, tags: ['public', 'secret'] }]"

[[test.results]]
value = "[{ body: 'scalar row', id: doc:scalar, tags: 'public' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: doc:scalar, tags: 'public' }]"

[[test.results]]
value = "[{ id: doc:scalar, tags: 'public' }]"

[[test.results]]
value = "{ attributes: { projections: 'id, tags' }, children: [{ attributes: { access: \"= 'public'\", direction: 'Forward', index: 'idx_tags' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"
*/

DEFINE TABLE doc SCHEMALESS;
CREATE doc:leak SET tags = ['public', 'secret'], body = 'array row';
CREATE doc:scalar SET tags = 'public', body = 'scalar row';
DEFINE INDEX idx_tags ON doc COLUMNS tags;

-- Indexed plan; must agree with NOINDEX (no array row leak).
SELECT id, tags FROM doc WHERE tags = 'public' ORDER BY id;

-- Reference: table-scan + predicate evaluation.
SELECT id, tags FROM doc WITH NOINDEX WHERE tags = 'public' ORDER BY id;

-- Confirm the index is actually chosen so the recheck path is exercised.
SELECT id, tags FROM doc WHERE tags = 'public' EXPLAIN;
