/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Full-text and bitmap scans that decode only the fields a statement reads return what a whole-record decode returns"
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
value = "[{ body: 'student report one', emb: [0.5f, 0.25f], grp: 'big', id: doc:1, n: 2 }]"

[[test.results]]
value = "[{ body: 'student two', emb: [0.75f, 0.125f], grp: 'big', id: doc:2, n: 1 }]"

[[test.results]]
value = "[{ emb: [0.5f, 0.25f], id: doc:1 }, { emb: [0.75f, 0.125f], id: doc:2 }]"

[[test.results]]
value = "[{ emb: [0.5f, 0.25f], id: doc:1 }, { emb: [0.75f, 0.125f], id: doc:2 }]"

[[test.results]]
value = "[{ e: [0.5f, 0.25f], id: doc:1 }, { e: [0.75f, 0.125f], id: doc:2 }]"

[[test.results]]
value = "[{ e: [0.5f], id: doc:1 }, { e: [0.75f], id: doc:2 }]"

[[test.results]]
value = "[{ id: doc:1 }]"

[[test.results]]
value = "[{ h: '<b>student</b> report one', id: doc:1 }, { h: '<b>student</b> two', id: doc:2 }]"

[[test.results]]
value = "[{ body: 'student report one', grp: 'big', id: doc:1, n: 2 }, { body: 'student two', grp: 'big', id: doc:2, n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'student', emb: [0.5f, 0.25f], first: 0.5f, id: doc2:1 }]"

[[test.results]]
value = "[{ first: 0.5f, id: doc2:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'student report one', emb: [1f], grp: 'big', id: doc:1, n: 2 }]"

[[test.results]]
value = "[{ emb: [1f], id: doc:1 }, { emb: [0.75f, 0.125f], id: doc:2 }]"

[[test.results]]
value = "NONE"
*/

DEFINE ANALYZER english TOKENIZERS blank, class, punct FILTERS lowercase, ascii, snowball(english);
DEFINE TABLE doc SCHEMALESS;
DEFINE INDEX doc_grp ON doc FIELDS grp;
DEFINE INDEX doc_body ON doc FIELDS body FULLTEXT ANALYZER english BM25 HIGHLIGHTS;
CREATE doc:1 SET grp = 'big', body = 'student report one', n = 2, emb = [0.5, 0.25];
CREATE doc:2 SET grp = 'big', body = 'student two', n = 1, emb = [0.75, 0.125];

-- A field the projection names is decoded.
SELECT id, emb FROM doc WHERE grp = 'big' AND (body @1@ 'student' OR body @2@ 'report') ORDER BY id;
SELECT id, emb FROM doc WHERE body @1@ 'student' ORDER BY id;
-- A field read through a projection function or a nested statement is read whole.
SELECT id, type::field('emb') AS e FROM doc WHERE body @1@ 'student' ORDER BY id;
SELECT id, (SELECT VALUE $parent.emb[0] FROM [1]) AS e FROM doc WHERE grp = 'big' AND (body @1@ 'student' OR body @2@ 'report') ORDER BY id;
-- A field only the condition reads still filters the rows.
SELECT id FROM doc WHERE grp = 'big' AND (body @1@ 'student' OR body @2@ 'report') AND n > 1 ORDER BY id;
-- Highlights read the indexed column.
SELECT id, search::highlight('<b>', '</b>', 1) AS h FROM doc WHERE body @1@ 'student' ORDER BY id;
-- A wildcard with OMIT decodes the rest of the record.
SELECT * OMIT emb FROM doc WHERE body @1@ 'student' ORDER BY id;

-- A computed field may read any stored field.
DEFINE TABLE doc2 SCHEMALESS;
DEFINE FIELD first ON doc2 COMPUTED emb[0];
DEFINE INDEX doc2_body ON doc2 FIELDS body FULLTEXT ANALYZER english BM25;
CREATE doc2:1 SET body = 'student', emb = [0.5, 0.25];
SELECT id, first FROM doc2 WHERE body @1@ 'student';

-- A transaction reads its own writes.
BEGIN;
UPDATE doc:1 SET emb = [1.0];
SELECT id, emb FROM doc WHERE body @1@ 'student' ORDER BY id;
COMMIT;
