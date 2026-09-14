/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro", "best-effort-ro"]
versioned = true

[test]
reason = "A reused full-text expression must respect each outer row's VERSION"
issue = 6811
results = [
  "NONE", "[]", "NONE", "NONE", "NONE", "NONE", "NONE", "NONE", "NONE", "[]",
  "[{ hits: [test:1] }, { hits: [] }]",
  { error = 'No full-text index found for field Idiom([Field("text")]) on table test' }
]
*/

DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
INSERT INTO test [
  { id: test:1, text: 'test' },
  { id: test:2, text: 'other' },
  { id: test:3, text: 'different' }
] RETURN NONE;
SLEEP 50ms;
LET $before = time::now();
SLEEP 50ms;
DEFINE INDEX search_text ON test FIELDS text FULLTEXT ANALYZER simple BM25;
SLEEP 50ms;
LET $after = time::now();
SLEEP 50ms;
CREATE checkpoint:1 RETURN NONE;

SELECT (SELECT VALUE id FROM [test:1] WHERE text @1@ 'test' VERSION $parent.ts) AS hits
FROM [{ ts: $after }, { ts: $before }];

-- Keep both rows so the score loader also encounters the historical missing index.
SELECT (SELECT VALUE search::score(1) FROM [test:1] WHERE text @1@ 'test' OR true VERSION $parent.ts) AS scores
FROM [{ ts: $after }, { ts: $before }];
