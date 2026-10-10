/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A full-text query inside a transaction reads the index as the transaction
	changed it: the documents it created, updated and deleted, before any of
	it is committed, on top of what earlier transactions committed. After the
	commit the same queries read the same answers from storage.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'the quick brown fox', id: doc:1 }]"

[[test.results]]
value = "[{ body: 'a lazy brown dog', id: doc:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'brown bread', id: doc:3 }]"

[[test.results]]
value = "[doc:1, doc:2, doc:3]"

[[test.results]]
value = "[{ body: 'the quick red fox', id: doc:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:3]"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "[doc:1, doc:3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[doc:3]"

[[test.results]]
value = "[doc:1, doc:3]"

[[test.results]]
value = "[]"
*/

DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX body ON doc FIELDS body FULLTEXT ANALYZER simple BM25;
CREATE doc:1 SET body = 'the quick brown fox';
CREATE doc:2 SET body = 'a lazy brown dog';
BEGIN;
CREATE doc:3 SET body = 'brown bread';
SELECT VALUE id FROM doc WHERE body @@ 'brown' ORDER BY id;
UPDATE doc:1 SET body = 'the quick red fox';
DELETE doc:2;
SELECT VALUE id FROM doc WHERE body @@ 'brown' ORDER BY id;
SELECT VALUE id FROM doc WHERE body @AND@ 'quick fox' ORDER BY id;
SELECT VALUE id FROM doc WHERE body @OR@ 'red bread' ORDER BY id;
SELECT VALUE id FROM doc WHERE body @@ 'lazy' ORDER BY id;
COMMIT;
SELECT VALUE id FROM doc WHERE body @@ 'brown' ORDER BY id;
SELECT VALUE id FROM doc WHERE body @OR@ 'red bread' ORDER BY id;
SELECT VALUE id FROM doc WHERE body @@ 'lazy' ORDER BY id;
