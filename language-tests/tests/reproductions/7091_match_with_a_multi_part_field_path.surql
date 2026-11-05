/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A full-text match over a nested field path is answered by the index"
issue = 7091

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, meta: { title: 'hello world' } }]"

[[test.results]]
value = "[{ id: t:2, meta: { title: 'other' } }]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { index: 'i', query: 'hello', table: 't' }, context: 'Db', operator: 'FullTextScan' }], context: 'Db', operator: 'ProjectValue' }"
*/

DEFINE ANALYZER a TOKENIZERS blank FILTERS lowercase;
DEFINE TABLE t SCHEMALESS;
DEFINE INDEX i ON t FIELDS meta.title FULLTEXT ANALYZER a BM25;
CREATE t:1 SET meta.title = "hello world";
CREATE t:2 SET meta.title = "other";
SELECT VALUE id FROM t WHERE meta.title @@ 'hello';
SELECT VALUE id FROM t WHERE meta.title @@ 'hello' EXPLAIN;
