/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An index function in the projection of a statement that SPLITs answers for the MATCHES its WHERE declares"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'hello world', id: doc:1, tags: ['a', 'b'] }]"

[[test.results]]
value = "[{ body: 'bye world', id: doc:2, tags: ['c'] }]"

[[test.results]]
value = "[{ id: doc:1, s: true, tags: 'a' }, { id: doc:1, s: true, tags: 'b' }]"

[[test.results]]
value = "[{ h: 'hello <world>', id: doc:1, tags: 'a' }, { h: 'hello <world>', id: doc:1, tags: 'b' }, { h: 'bye <world>', id: doc:2, tags: 'c' }]"
*/
DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
DEFINE INDEX ft ON doc FIELDS body FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
CREATE doc:1 SET body = 'hello world', tags = ['a', 'b'];
CREATE doc:2 SET body = 'bye world', tags = ['c'];
SELECT id, search::score(1) > 0 AS s, tags FROM doc WHERE body @1@ 'hello' SPLIT tags;
SELECT id, search::highlight('<', '>', 1) AS h, tags FROM doc WHERE body @1@ 'world' SPLIT tags ORDER BY tags;
