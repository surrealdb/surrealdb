/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "search::score over a BM25 index is positive and ranks test_docs:3 (2 occurrences in 3 words) ahead of test_docs:1 (3 occurrences in 6 words) under BM25 length normalization (b = 0.75)"
issue = 7290

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ content: 'apple banana cherry apple apple banana', id: test_docs:1 }]"

[[test.results]]
value = "[{ content: 'cherry date elderberry fig', id: test_docs:2 }]"

[[test.results]]
value = "[{ content: 'apple grape apple', id: test_docs:3 }]"

[[test.results]]
value = "[{ id: test_docs:1, positive: true }, { id: test_docs:3, positive: true }]"

[[test.results]]
value = "[test_docs:3, test_docs:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ content: 'apple pear plum fig', id: all_docs:1 }]"

[[test.results]]
value = "[{ content: 'apple apple apple grape', id: all_docs:2 }]"

[[test.results]]
value = "[{ id: all_docs:1, positive: true }, { id: all_docs:2, positive: true }]"

[[test.results]]
value = "[all_docs:2, all_docs:1]"
*/

DEFINE ANALYZER test_analyzer TOKENIZERS class FILTERS snowball(english), lowercase;
DEFINE TABLE test_docs SCHEMAFULL;
DEFINE FIELD content ON test_docs TYPE string;
DEFINE INDEX idx_content_ft ON test_docs FIELDS content FULLTEXT ANALYZER test_analyzer BM25;
CREATE test_docs:1 CONTENT { content: 'apple banana cherry apple apple banana' };
CREATE test_docs:2 CONTENT { content: 'cherry date elderberry fig' };
CREATE test_docs:3 CONTENT { content: 'apple grape apple' };
SELECT id, search::score(0) > 0 AS positive FROM test_docs WHERE content @0@ 'apple' ORDER BY id;
SELECT VALUE id FROM (SELECT id, search::score(0) AS score FROM test_docs WHERE content @0@ 'apple') ORDER BY score DESC;
-- A term every document carries still scores above zero and ranks by frequency.
DEFINE TABLE all_docs SCHEMAFULL;
DEFINE FIELD content ON all_docs TYPE string;
DEFINE INDEX idx_all ON all_docs FIELDS content FULLTEXT ANALYZER test_analyzer BM25;
CREATE all_docs:1 CONTENT { content: 'apple pear plum fig' };
CREATE all_docs:2 CONTENT { content: 'apple apple apple grape' };
SELECT id, search::score(0) > 0 AS positive FROM all_docs WHERE content @0@ 'apple' ORDER BY id;
SELECT VALUE id FROM (SELECT id, search::score(0) AS score FROM all_docs WHERE content @0@ 'apple') ORDER BY score DESC;
