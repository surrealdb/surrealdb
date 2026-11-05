/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A MATCHES over rows no full table source produced — a record id, an array of records, a record range, a graph hop, an object holding an id — judges each row's own value with the analyzer of the table's full-text index"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: doc:1, text: 'the quick brown fox' }, { id: doc:2, text: 'a lazy dog' }, { id: doc:3, text: 'quick thinking' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:1, doc:3]"

[[test.results]]
value = "[doc:1, doc:3]"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "[doc:3]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[doc:1, doc:3]"
*/

DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
DEFINE TABLE doc;
DEFINE INDEX doc_text ON doc FIELDS text FULLTEXT ANALYZER simple BM25;
DEFINE TABLE link;
INSERT INTO doc [{ id: 1, text: 'the quick brown fox' }, { id: 2, text: 'a lazy dog' }, { id: 3, text: 'quick thinking' }];
RELATE [user:a]->link->[doc:1, doc:2] RETURN NONE;
SELECT VALUE id FROM doc:1 WHERE text @@ 'quick';
SELECT VALUE id FROM doc:2 WHERE text @@ 'quick';
SELECT VALUE id FROM [doc:1, doc:2, doc:3] WHERE text @@ 'quick';
SELECT VALUE id FROM doc:1..=3 WHERE text @@ 'QUICK';
SELECT VALUE id FROM [doc:1, doc:2, doc:3] WHERE text @@ 'quick brown';
SELECT VALUE id FROM [doc:1, doc:2, doc:3] WHERE text @@ 'thinking';
SELECT VALUE id FROM user:a->link->doc WHERE text @@ 'dog';
-- A row is judged on the value it holds, not on the record its id names.
SELECT VALUE id FROM [{ id: doc:2, text: 'quick' }] WHERE text @@ 'quick';
SELECT VALUE id FROM [{ id: doc:1, text: 'slow' }] WHERE text @@ 'quick';
LET $q = 'quick';
SELECT VALUE id FROM [doc:1, doc:2, doc:3] WHERE text @@ $q;
