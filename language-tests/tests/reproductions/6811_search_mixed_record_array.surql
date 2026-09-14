/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "Full-text matching and search functions use each record's table"
issue = 6811
results = [
  "NONE", "NONE", "NONE", "[]", "[]", "[]",
  "NONE", "NONE", "true", "true",
  "[unknown:1, other:1]", "[other:1, unknown:1]",
  "true", "true", "true", "true"
]
*/

DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
DEFINE INDEX search_text ON unknown FIELDS text FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
DEFINE INDEX search_text ON other FIELDS text FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
INSERT INTO unknown [
  { id: unknown:1, text: 'test' },
  { id: unknown:2, text: 'other' },
  { id: unknown:3, text: 'different' }
] RETURN NONE;
INSERT INTO other [
  { id: other:1, text: 'TEST' },
  { id: other:2, text: 'two' },
  { id: other:3, text: 'three' },
  { id: other:4, text: 'four' },
  { id: other:5, text: 'five' }
] RETURN NONE;
CREATE unindexed:1 SET text = 'test' RETURN NONE;

LET $first = SELECT id, search::score(1) AS score, search::highlight('<b>', '</b>', 1) AS highlight, search::offsets(1) AS offsets
  FROM unknown WHERE text @1@ 'test';
LET $second = SELECT id, search::score(1) AS score, search::highlight('<b>', '</b>', 1) AS highlight, search::offsets(1) AS offsets
  FROM other WHERE text @1@ 'test';
RETURN $first = [{ id: unknown:1, score: 0.5108256340026855f, highlight: '<b>test</b>', offsets: { '0': [{ s: 0, e: 4 }] } }];
-- Five one-word documents with one match give ln(3), rounded to f32.
RETURN $second = [{ id: other:1, score: 1.0986123085021973f, highlight: '<b>TEST</b>', offsets: { '0': [{ s: 0, e: 4 }] } }];

SELECT VALUE id FROM [unknown:1, other:1] WHERE text @1@ 'test';
SELECT VALUE id FROM [other:1, unknown:1] WHERE text @1@ 'test';
RETURN (SELECT id, search::score(1) AS score, search::highlight('<b>', '</b>', 1) AS highlight, search::offsets(1) AS offsets
  FROM [unknown:1, other:1, unknown:2] WHERE text @1@ 'test') = array::concat($first, $second);
RETURN (SELECT id, search::score(1) AS score, search::highlight('<b>', '</b>', 1) AS highlight, search::offsets(1) AS offsets
  FROM [other:1, unknown:1, other:2] WHERE text @1@ 'test') = array::concat($second, $first);
RETURN (SELECT id, search::score(1) AS score, search::highlight('<b>', '</b>', 1) AS highlight, search::offsets(1) AS offsets
  FROM [unindexed:1, other:1, unknown:1] WHERE text @1@ 'test') = array::concat($second, $first);
RETURN (SELECT id, search::score(1) AS score, search::highlight('<b>', '</b>', 1) AS highlight, search::offsets(1) AS offsets
  FROM [unknown:1, unindexed:1, other:1, unknown:1] WHERE text @1@ 'test') = array::concat($first, $second, $first);
