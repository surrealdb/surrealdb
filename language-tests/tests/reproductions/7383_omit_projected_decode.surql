/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Scans under SELECT * OMIT that skip decoding the omitted fields return what a whole-record decode returns"
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
value = "[{ description: 'a report', embedding: [0.5f, 0.25f], grp: 'a', id: post:1, meta: { x: 1, y: 2 }, n: 1, status: 'visible', title: 'student loans' }]"

[[test.results]]
value = "[{ description: 'student report', embedding: [0.75f, 0.125f], grp: 'a', id: post:2, meta: { x: 3, y: 4 }, n: 2, title: 'campus news' }]"

[[test.results]]
value = "[{ description: 'notes', embedding: [1.5f], grp: 'b', id: post:3, meta: { x: 5, y: 6 }, n: 3, status: 'hidden', title: 'student life' }]"

[[test.results]]
value = "[{ description: 'a report', grp: 'a', id: post:1, meta: { x: 1, y: 2 }, n: 1, status: 'visible', title: 'student loans' }, { description: 'notes', grp: 'b', id: post:3, meta: { x: 5, y: 6 }, n: 3, status: 'hidden', title: 'student life' }]"

[[test.results]]
value = "[{ description: 'a report', grp: 'a', id: post:1, meta: { x: 1, y: 2 }, n: 1, status: 'visible', title: 'student loans' }, { description: 'student report', grp: 'a', id: post:2, meta: { x: 3, y: 4 }, n: 2, title: 'campus news' }]"

[[test.results]]
value = "[{ description: 'a report', grp: 'a', id: post:1, meta: { x: 1, y: 2 }, n: 1, status: 'visible', title: 'student loans' }, { description: 'student report', grp: 'a', id: post:2, meta: { x: 3, y: 4 }, n: 2, title: 'campus news' }]"

[[test.results]]
value = "[{ _score: 0.4721917510032654f, description: 'student report', grp: 'a', id: post:2, meta: { x: 3, y: 4 }, n: 2, title: 'campus news' }, { _score: 0f, description: 'a report', grp: 'a', id: post:1, meta: { x: 1, y: 2 }, n: 1, status: 'visible', title: 'student loans' }]"

[[test.results]]
value = "[{ description: 'student report', grp: 'a', id: post:2, meta: { x: 3, y: 4 }, n: 2, title: 'campus news' }]"

[[test.results]]
value = "[{ description: 'a report', e: 0.5f, grp: 'a', id: post:1, meta: { x: 1, y: 2 }, n: 1, status: 'visible', title: 'student loans' }, { description: 'student report', e: 0.75f, grp: 'a', id: post:2, meta: { x: 3, y: 4 }, n: 2, title: 'campus news' }]"

[[test.results]]
value = "[{ description: 'a report', embedding: [0.5f, 0.25f], grp: 'a', id: post:1, meta: { y: 2 }, n: 1, status: 'visible', title: 'student loans' }, { description: 'student report', embedding: [0.75f, 0.125f], grp: 'a', id: post:2, meta: { y: 4 }, n: 2, title: 'campus news' }]"

[[test.results]]
value = "[{ description: 'a report', grp: 'a', id: post:1, n: 1, status: 'visible', title: 'student loans' }, { description: 'student report', grp: 'a', id: post:2, n: 2, title: 'campus news' }]"

[[test.results]]
value = "[{ description: 'student report', grp: 'a', id: post:2, meta: { x: 3, y: 4 }, n: 2, title: 'campus news' }, { description: 'a report', grp: 'a', id: post:1, meta: { x: 1, y: 2 }, n: 1, status: 'visible', title: 'student loans' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ embedding: [0.5f, 0.25f], first: 0.5f, grp: 'a', id: post2:1 }]"

[[test.results]]
value = "[{ first: 0.5f, grp: 'a', id: post2:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ description: 'a report', embedding: [9f], grp: 'a', id: post:1, meta: { x: 1, y: 2 }, n: 1, status: 'visible', title: 'student update' }]"

[[test.results]]
value = "[{ description: 'a report', grp: 'a', id: post:1, meta: { x: 1, y: 2 }, n: 1, status: 'visible', title: 'student update' }, { description: 'student report', grp: 'a', id: post:2, meta: { x: 3, y: 4 }, n: 2, title: 'campus news' }]"

[[test.results]]
value = "NONE"
*/

DEFINE ANALYZER english TOKENIZERS blank, class, punct FILTERS lowercase, ascii, snowball(english);
DEFINE TABLE post SCHEMALESS;
DEFINE INDEX post_grp ON post FIELDS grp;
DEFINE INDEX post_title ON post FIELDS title FULLTEXT ANALYZER english BM25(1.2,0.75);
DEFINE INDEX post_description ON post FIELDS description FULLTEXT ANALYZER english BM25(1.2,0.75);
CREATE post:1 SET grp = 'a', title = 'student loans', description = 'a report', status = 'visible', n = 1, meta = { x: 1, y: 2 }, embedding = [0.5, 0.25];
CREATE post:2 SET grp = 'a', title = 'campus news', description = 'student report', n = 2, meta = { x: 3, y: 4 }, embedding = [0.75, 0.125];
CREATE post:3 SET grp = 'b', title = 'student life', description = 'notes', status = 'hidden', n = 3, meta = { x: 5, y: 6 }, embedding = [1.5];

-- Full-text, bitmap and index scans under `* OMIT`.
SELECT * OMIT embedding FROM post WHERE title @1@ 'student' ORDER BY id;
SELECT * OMIT embedding FROM post WHERE grp = 'a' AND (title @1@ 'student' OR description @2@ 'student') ORDER BY id;
SELECT * OMIT embedding FROM post WHERE grp = 'a' ORDER BY id;
-- The reporter's ranked shape.
SELECT *, search::score(1) + search::score(2) AS _score OMIT embedding FROM post
	WHERE (title @1@ 'student' OR description @2@ 'student') AND (status IS NONE OR status = 'visible')
	ORDER BY _score DESC LIMIT 3;
-- An omitted field another clause reads is still read.
SELECT * OMIT embedding FROM post WHERE grp = 'a' AND embedding[0] > 0.6 ORDER BY id;
SELECT *, embedding[0] AS e OMIT embedding FROM post WHERE grp = 'a' ORDER BY id;
-- A nested OMIT, several OMIT targets, and an ordering under OMIT.
SELECT * OMIT meta.x FROM post WHERE grp = 'a' ORDER BY id;
SELECT * OMIT embedding, meta FROM post WHERE grp = 'a' ORDER BY id;
SELECT * OMIT embedding FROM post WHERE grp = 'a' ORDER BY n DESC;

-- A computed field may read an omitted field.
DEFINE TABLE post2 SCHEMALESS;
DEFINE FIELD first ON post2 COMPUTED embedding[0];
DEFINE INDEX post2_grp ON post2 FIELDS grp;
CREATE post2:1 SET grp = 'a', embedding = [0.5, 0.25];
SELECT * OMIT embedding FROM post2 WHERE grp = 'a';

-- A transaction reads its own writes.
BEGIN;
UPDATE post:1 SET title = 'student update', embedding = [9.0];
SELECT * OMIT embedding FROM post WHERE grp = 'a' ORDER BY id;
COMMIT;
