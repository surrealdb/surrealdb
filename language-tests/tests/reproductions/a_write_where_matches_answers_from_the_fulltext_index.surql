/**
[env]
namespace = true
database = true

[test]
reason = "A MATCHES in the WHERE of an UPDATE, UPSERT or DELETE is answered from the table's full-text index, as in a SELECT: the write reaches the matching records, an UPSERT that matches one writes no new record, and a record-id target or a table with no full-text index answers as a SELECT does."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'hello world', id: doc:1 }]"

[[test.results]]
value = "[{ body: 'bye', id: doc:2 }]"

[[test.results]]
value = "[{ body: 'hello there', id: doc:3 }]"

[[test.results]]
value = "[doc:1, doc:3]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[doc:1, doc:2, doc:3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "[{ body: 'hello world', hit: true, id: doc:1, x: 1, z: 1 }, { body: 'hello there', hit: true, id: doc:3, x: 1 }]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[{ body: 'world', id: nofts:1 }]"

[[test.results]]
error = "There was no suitable index supporting the expression: body @@ 'world'"

*/

DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft ON doc FIELDS body FULLTEXT ANALYZER simple BM25;
CREATE doc:1 SET body = 'hello world';
CREATE doc:2 SET body = 'bye';
CREATE doc:3 SET body = 'hello there';
UPDATE doc SET hit = true WHERE body @@ 'hello' RETURN VALUE id;
UPSERT doc SET u = true WHERE body @@ 'bye' RETURN VALUE id;
UPDATE doc SET x = 1 WHERE body @@ 'hello world' OR id = doc:2 RETURN VALUE id;
UPDATE doc:2 SET y = 1 WHERE body @@ 'bye' RETURN VALUE id;
UPDATE doc SET z = 1 WHERE string::len(body) > 0 AND body @@ 'world' RETURN VALUE id;
DELETE doc WHERE body @@ 'hello' RETURN BEFORE;
SELECT VALUE id FROM doc ORDER BY id;
CREATE nofts:1 SET body = 'world';
UPDATE nofts SET z = 1 WHERE body @@ 'world' RETURN VALUE id;
