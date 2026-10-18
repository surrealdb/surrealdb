/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A full-text match applied to an array of record ids filters by the match"
issue = 6811
wip = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:test1, test: 'test' }]"

[[test.results]]
value = "[{ id: test:test2, test: 'hahaha' }]"

[[test.results]]
value = "[{ id: test:test3, test: 'kevin' }]"

[[test.results]]
value = "[test:test1]"

[[test.results]]
value = "[test:test1]"
*/

DEFINE ANALYZER search_analyzer TOKENIZERS class, blank, punct, camel FILTERS lowercase, ascii, ngram(1,5), edgengram(1,10);
DEFINE TABLE test;
DEFINE INDEX search_index ON test FIELDS test FULLTEXT ANALYZER search_analyzer BM25;
CREATE test:test1 SET test = "test";
CREATE test:test2 SET test = "hahaha";
CREATE test:test3 SET test = "kevin";
SELECT VALUE id FROM [test:test1, test:test2, test:test3] WHERE test @@ 'test';
SELECT VALUE id FROM test WHERE test @@ 'test';
