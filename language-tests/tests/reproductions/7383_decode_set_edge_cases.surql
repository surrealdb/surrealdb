/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Scans that decode only part of each record return what a whole-record decode returns under FETCH, aliases and mixed OMIT targets"
issue = 7383

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:ann, name: 'Ann' }]"

[[test.results]]
value = "[{ id: person:bob, name: 'Bob' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ author: person:ann, embedding: [0.5f, 0.25f], grp: 'a', id: post:1, meta: { x: 1, y: 2 }, secret: 's1', title: 'one' }]"

[[test.results]]
value = "[{ author: person:bob, embedding: [0.75f], grp: 'a', id: post:2, meta: { x: 3, y: 4 }, secret: 's2', title: 'two' }]"

[[test.results]]
value = "[{ id: post:1, title: 'one' }, { id: post:2, title: 'two' }]"

[[test.results]]
value = "[{ author: { id: person:ann, name: 'Ann' }, id: post:1 }, { author: { id: person:bob, name: 'Bob' }, id: post:2 }]"

[[test.results]]
value = "[{ author: { id: person:ann, name: 'Ann' }, grp: 'a', id: post:1, meta: { x: 1, y: 2 }, secret: 's1', title: 'one' }, { author: { id: person:bob, name: 'Bob' }, grp: 'a', id: post:2, meta: { x: 3, y: 4 }, secret: 's2', title: 'two' }]"

[[test.results]]
value = "[{ embedding: [0.5f, 0.25f], grp: 'a', id: post:1, meta: { x: 1, y: 2 }, secret: 's1', title: 'one' }, { embedding: [0.75f], grp: 'a', id: post:2, meta: { x: 3, y: 4 }, secret: 's2', title: 'two' }]"

[[test.results]]
value = "[{ author: person:ann, grp: 'a', id: post:1, meta: { y: 2 }, secret: 's1', title: 'one' }, { author: person:bob, grp: 'a', id: post:2, meta: { y: 4 }, secret: 's2', title: 'two' }]"

[[test.results]]
value = "[{ author: person:ann, embedding: [0.5f, 0.25f], grp: 'a', id: post:1, meta: { x: 1, y: 2 }, title: 'one' }, { author: person:bob, embedding: [0.75f], grp: 'a', id: post:2, meta: { x: 3, y: 4 }, title: 'two' }]"

[[test.results]]
value = "[{ author: person:ann, grp: 'a', id: post:1, meta: { x: 1, y: 2 }, secret: 1, title: 'one' }, { author: person:bob, grp: 'a', id: post:2, meta: { x: 3, y: 4 }, secret: 1, title: 'two' }]"

[[test.results]]
value = "[{ one: 1 }, { one: 1 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ author: person:ann, embedding: [0.5f, 0.25f], grp: 'a', meta: { x: 1, y: 2 }, secret: 's1', title: 'one' }, { author: person:bob, embedding: [0.75f], grp: 'a', meta: { x: 3, y: 4 }, secret: 's2', title: 'two' }]"

[[test.results]]
value = "[{ author: person:ann, grp: 'a', meta: { x: 1, y: 2 }, secret: 's1', title: 'one' }, { author: person:bob, grp: 'a', meta: { x: 3, y: 4 }, secret: 's2', title: 'two' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ emb: [1f], id: u:1, slug: 'a', title: 'one' }]"

[[test.results]]
value = "[{ emb: [2f], id: u:2, title: 'two' }]"

[[test.results]]
value = "[{ emb: [3f], id: u:3, title: 'three' }]"

[[test.results]]
value = "[{ emb: [4f], id: u:4, slug: NULL, title: 'four' }]"

[[test.results]]
value = "[{ emb: [5f], id: u:5, slug: NULL, title: 'five' }]"

[[test.results]]
value = "[{ id: u:2, title: 'two' }, { id: u:3, title: 'three' }]"

[[test.results]]
value = "[{ id: u:4, title: 'four' }, { id: u:5, title: 'five' }]"

[[test.results]]
value = "[{ id: u:2, title: 'two' }, { id: u:3, title: 'three' }]"

*/

DEFINE TABLE person SCHEMALESS;
CREATE person:ann SET name = 'Ann';
CREATE person:bob SET name = 'Bob';
DEFINE TABLE post SCHEMALESS;
DEFINE INDEX post_grp ON post FIELDS grp;
CREATE post:1 SET grp = 'a', title = 'one', author = person:ann, secret = 's1', embedding = [0.5, 0.25], meta = { x: 1, y: 2 };
CREATE post:2 SET grp = 'a', title = 'two', author = person:bob, secret = 's2', embedding = [0.75], meta = { x: 3, y: 4 };

-- FETCH on a projected decode and under * OMIT.
SELECT id, title FROM post WHERE grp = 'a' ORDER BY id FETCH author;
SELECT id, author FROM post WHERE grp = 'a' ORDER BY id FETCH author;
SELECT * OMIT embedding FROM post WHERE grp = 'a' ORDER BY id FETCH author;
SELECT * OMIT author FROM post WHERE grp = 'a' ORDER BY id FETCH author;
-- A top-level and a nested OMIT target together.
SELECT * OMIT embedding, meta.x FROM post WHERE grp = 'a' ORDER BY id;
-- An alias named like a stored field the statement omits.
SELECT *, 1 AS secret OMIT secret FROM post WHERE grp = 'a' ORDER BY id;
SELECT *, 1 AS secret OMIT embedding FROM post WHERE grp = 'a' ORDER BY id;
-- A projection that reads no stored field.
SELECT 1 AS one FROM post WHERE grp = 'a';
SELECT count() FROM post WHERE grp = 'a' GROUP ALL;
-- `id` is set from the storage key, so omitting it excludes nothing stored.
SELECT * OMIT id FROM post WHERE grp = 'a' ORDER BY title;
SELECT * OMIT id, embedding FROM post WHERE grp = 'a' ORDER BY title;
-- NONE and NULL on a unique index match every record holding them.
DEFINE TABLE u SCHEMALESS;
DEFINE INDEX u_slug ON u FIELDS slug UNIQUE;
CREATE u:1 SET slug = 'a', title = 'one', emb = [1.0];
CREATE u:2 SET title = 'two', emb = [2.0];
CREATE u:3 SET title = 'three', emb = [3.0];
CREATE u:4 SET slug = NULL, title = 'four', emb = [4.0];
CREATE u:5 SET slug = NULL, title = 'five', emb = [5.0];
SELECT id, title FROM u WHERE slug = NONE ORDER BY id;
SELECT id, title FROM u WHERE slug = NULL ORDER BY id;
SELECT * OMIT emb FROM u WHERE slug = NONE ORDER BY id;
