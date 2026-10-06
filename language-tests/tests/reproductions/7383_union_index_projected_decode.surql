/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Union index scans whose sub-scans decode only the fields a statement reads return what a whole-record decode returns"
issue = 7383

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ emb: [0.5f, 0.25f], grp: 'a', id: t:1, label: 'x', n: 1, slug: 's1' }]"

[[test.results]]
value = "[{ emb: [0.75f, 0.125f], grp: 'b', id: t:2, label: 'y', n: 2, slug: 's2' }]"

[[test.results]]
value = "[{ emb: [1.5f], grp: 'c', id: t:3, label: 'y', n: 3, slug: 's3' }]"

[[test.results]]
value = "[{ emb: [0.5f, 0.25f], id: t:1 }, { emb: [0.75f, 0.125f], id: t:2 }]"

[[test.results]]
value = "[{ id: t:1, label: 'x' }, { id: t:3, label: 'y' }]"

[[test.results]]
value = "[{ id: t:3, n: 3 }, { id: t:1, n: 1 }]"

[[test.results]]
value = "[{ grp: 'a', id: t:1 }, { grp: 'b', id: t:2 }, { grp: 'c', id: t:3 }]"

[[test.results]]
value = "[{ grp: 'a', id: t:1, label: 'x', n: 1, slug: 's1' }, { grp: 'b', id: t:2, label: 'y', n: 2, slug: 's2' }]"

[[test.results]]
value = "[{ id: t:2 }]"

[[test.results]]
value = "[{ e: [0.5f, 0.25f], id: t:1 }, { e: [0.75f, 0.125f], id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ emb: [0.5f, 0.25f], first: 0.5f, grp: 'a', id: t2:1 }]"

[[test.results]]
value = "[{ emb: [0.75f], first: 0.75f, grp: 'b', id: t2:2 }]"

[[test.results]]
value = "[{ first: 0.5f, id: t2:1 }, { first: 0.75f, id: t2:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ emb: [9f], grp: 'a', id: t:1, label: 'x', n: 1, slug: 's1' }]"

[[test.results]]
value = "[{ emb: [9f], id: t:1 }, { emb: [0.75f, 0.125f], id: t:2 }]"

[[test.results]]
value = "NONE"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX t_grp ON t FIELDS grp;
DEFINE INDEX t_slug ON t FIELDS slug UNIQUE;
CREATE t:1 SET grp = 'a', slug = 's1', n = 1, label = 'x', emb = [0.5, 0.25];
CREATE t:2 SET grp = 'b', slug = 's2', n = 2, label = 'y', emb = [0.75, 0.125];
CREATE t:3 SET grp = 'c', slug = 's3', n = 3, label = 'y', emb = [1.5];

-- An IN list and an OR over two indexes, each a union of index scans.
SELECT id, emb FROM t WHERE grp IN ['a', 'b'] ORDER BY id;
SELECT id, label FROM t WHERE grp = 'a' OR slug = 's3' ORDER BY id;
-- The record-id merge and the index-key merge.
SELECT id, n FROM t WHERE grp IN ['a', 'c'] ORDER BY id DESC;
SELECT id, grp FROM t WHERE grp IN ['c', 'a', 'b'] ORDER BY grp;
-- A wildcard with OMIT, and a field only the condition reads.
SELECT * OMIT emb FROM t WHERE grp IN ['a', 'b'] ORDER BY id;
SELECT id FROM t WHERE (grp = 'a' OR slug = 's2') AND label = 'y' ORDER BY id;
-- A projection function reads the record whole.
SELECT id, type::field('emb') AS e FROM t WHERE grp IN ['a', 'b'] ORDER BY id;

-- A computed field may read any stored field.
DEFINE TABLE t2 SCHEMALESS;
DEFINE FIELD first ON t2 COMPUTED emb[0];
DEFINE INDEX t2_grp ON t2 FIELDS grp;
CREATE t2:1 SET grp = 'a', emb = [0.5, 0.25];
CREATE t2:2 SET grp = 'b', emb = [0.75];
SELECT id, first FROM t2 WHERE grp IN ['a', 'b'] ORDER BY id;

-- A transaction reads its own writes.
BEGIN;
UPDATE t:1 SET emb = [9.0];
SELECT id, emb FROM t WHERE grp IN ['a', 'b'] ORDER BY id;
COMMIT;
