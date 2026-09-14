/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "Full-text scores from record arrays agree with table queries"
issue = 6811
results = [
  "NONE", "NONE", "[]", "NONE",
  "[{ id: test:1, score: 0.5108256340026855f }]",
  "[{ id: test:1, score: 0.5108256340026855f }]",
  "true", "NONE", "true", "true", "[]", "[]", "[]",
  "[{ highlight: '<b>test</b>', offsets: { '0': [{ s: 0, e: 4 }] } }]"
]
*/

DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
DEFINE INDEX search_text ON test FIELDS text FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
INSERT INTO test [
  { id: test:1, text: 'test' },
  { id: test:2, text: 'other' },
  { id: test:3, text: 'different' }
] RETURN NONE;

LET $expected = SELECT id, search::score(1) AS score FROM test WHERE text @1@ 'test';
-- With three one-word documents and one match, BM25 is ln(5/3), rounded to f32.
RETURN $expected;
SELECT id, search::score(1) AS score FROM [test:1, test:2, test:3] WHERE text @1@ 'test';
RETURN (SELECT id, search::score(1) AS score FROM test:1 WHERE text @1@ 'test') = $expected;
LET $records = [test:3, test:1, test:2];
RETURN (SELECT id, search::score(1) AS score FROM $records WHERE text @1@ 'test') = $expected;
RETURN (SELECT id, search::score(1) AS score FROM [test:1, test:1] WHERE text @1@ 'test') = array::concat($expected, $expected);
SELECT search::score(1) FROM [] WHERE text @1@ 'test';
SELECT search::score(1) FROM [test:missing] WHERE text @1@ 'test';
SELECT search::score(1) FROM [{ text: 'test' }] WHERE text @1@ 'test';
SELECT search::highlight('<b>', '</b>', 1) AS highlight, search::offsets(1) AS offsets
FROM [test:1, test:2] WHERE text @1@ 'test';
