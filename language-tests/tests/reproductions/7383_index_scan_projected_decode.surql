/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Index scans that decode only the fields a statement reads return what a whole-record decode returns"
issue = 7383

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
value = "[{ cat: 'c', emb: [0.5f, 0.25f], grp: 'a', id: t:1, label: 'x', n: 1, slug: 's1' }]"

[[test.results]]
value = "[{ cat: 'c', emb: [0.75f, 0.125f], grp: 'a', id: t:2, label: 'y', n: 2, slug: 's2' }]"

[[test.results]]
value = "[{ cat: 'd', emb: [1.5f], grp: 'b', id: t:3, label: 'y', n: 3, slug: 's3' }]"

[[test.results]]
value = "[{ emb: [0.5f, 0.25f], id: t:1 }, { emb: [0.75f, 0.125f], id: t:2 }]"

[[test.results]]
value = "[{ id: t:1, label: 'x' }, { id: t:2, label: 'y' }]"

[[test.results]]
value = "[{ emb: [0.75f, 0.125f], id: t:2 }]"

[[test.results]]
value = "[{ emb: [0.75f, 0.125f], id: t:2 }, { emb: [1.5f], id: t:3 }]"

[[test.results]]
value = "[{ emb: [0.75f, 0.125f], id: t:2 }]"

[[test.results]]
value = "[{ id: t:2, label: 'y' }]"

[[test.results]]
value = "[{ e: [0.5f, 0.25f], id: t:1 }, { e: [0.75f, 0.125f], id: t:2 }]"

[[test.results]]
value = "[{ id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ emb: [0.5f, 0.25f], first: 0.5f, grp: 'a', id: t2:1 }]"

[[test.results]]
value = "[{ first: 0.5f, id: t2:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ cat: 'c', emb: [9f], grp: 'a', id: t:1, label: 'x', n: 1, slug: 's1' }]"

[[test.results]]
value = "[{ emb: [9f], id: t:1 }, { emb: [0.75f, 0.125f], id: t:2 }]"

[[test.results]]
value = "NONE"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX t_grp ON t FIELDS grp;
DEFINE INDEX t_slug ON t FIELDS slug UNIQUE;
DEFINE INDEX t_n ON t FIELDS n;
DEFINE INDEX t_cat_n ON t FIELDS cat, n;
CREATE t:1 SET grp = 'a', slug = 's1', cat = 'c', n = 1, label = 'x', emb = [0.5, 0.25];
CREATE t:2 SET grp = 'a', slug = 's2', cat = 'c', n = 2, label = 'y', emb = [0.75, 0.125];
CREATE t:3 SET grp = 'b', slug = 's3', cat = 'd', n = 3, label = 'y', emb = [1.5];

-- Non-unique equality: a field the projection names is decoded, one it does not is not returned.
SELECT id, emb FROM t WHERE grp = 'a' ORDER BY id;
SELECT id, label FROM t WHERE grp = 'a' ORDER BY id;
-- Unique equality.
SELECT id, emb FROM t WHERE slug = 's2';
-- Range.
SELECT id, emb FROM t WHERE n > 1 ORDER BY id;
-- Compound equality and compound range.
SELECT id, emb FROM t WHERE cat = 'c' AND n = 2;
SELECT id, label FROM t WHERE cat = 'c' AND n > 1 ORDER BY id;
-- A projection function reads the record whole.
SELECT id, type::field('emb') AS e FROM t WHERE grp = 'a' ORDER BY id;
-- A field only the condition reads still filters the rows.
SELECT id FROM t WHERE grp = 'a' AND label = 'y' ORDER BY id;

-- A computed field may read any stored field.
DEFINE TABLE t2 SCHEMALESS;
DEFINE FIELD first ON t2 COMPUTED emb[0];
DEFINE INDEX t2_grp ON t2 FIELDS grp;
CREATE t2:1 SET grp = 'a', emb = [0.5, 0.25];
SELECT id, first FROM t2 WHERE grp = 'a';

-- A transaction reads its own writes.
BEGIN;
UPDATE t:1 SET emb = [9.0];
SELECT id, emb FROM t WHERE grp = 'a' ORDER BY id;
COMMIT;
