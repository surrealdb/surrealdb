/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ content: 'respiratory fever symptoms', id: test_doc:1 }]"

[[test.results]]
value = "[{ content: 'cardiac arrest emergency', id: test_doc:2 }]"

[[test.results]]
value = "[{ content: 'respiratory distress syndrome', id: test_doc:3 }]"

[[test.results]]
value = "[{ id: test_doc:1, score: 0f }, { id: test_doc:3, score: 0f }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test_doc:1, score: 0f }, { id: test_doc:3, score: 0f }]"

*/

-- Setup
DEFINE ANALYZER OVERWRITE test_analyzer TOKENIZERS blank, class FILTERS lowercase, ascii, snowball(english);
DEFINE TABLE test_doc SCHEMAFULL;
DEFINE FIELD content ON test_doc TYPE string;
DEFINE INDEX idx_test_content ON test_doc FIELDS content FULLTEXT ANALYZER test_analyzer BM25;

CREATE test_doc:1 SET content = 'respiratory fever symptoms';
CREATE test_doc:2 SET content = 'cardiac arrest emergency';
CREATE test_doc:3 SET content = 'respiratory distress syndrome';

-- ✅ This works (top-level query):
SELECT id, search::score(1) AS score
  FROM test_doc
  WHERE content @1@ 'respiratory'
  ORDER BY score DESC;
-- Returns: [{id: test_doc:1, score: ...}, {id: test_doc:3, score: ...}]

-- ✅ Function definition succeeds:
DEFINE FUNCTION fn::search_docs($query: string) {
  LET $results = (SELECT id, search::score(1) AS score
    FROM test_doc
    WHERE content @1@ $query
    ORDER BY score DESC);
  RETURN $results;
};

-- ❌ Calling the function fails:
RETURN fn::search_docs('respiratory');
-- Error: "Invalid query: Index function 'search::score': no MATCHES clause found in WHERE condition"