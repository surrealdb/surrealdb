/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "search::score() and @N@ match operator must work inside DEFINE FUNCTION"
issue = 7013

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1, text: 'IAM vision: Agents have identity and authority scopes' }]"

[[test.results]]
value = "[{ id: test:2, text: 'Governance model: Feed is the single governance surface' }]"

[[test.results]]
value = "[{ id: test:3, text: 'Target market is solo founders and tiny teams' }]"

[[test.results]]
value = "[{ id: test:2, score: 0.6499114632606506f, text: 'Governance model: Feed is the single governance surface' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:2, score: 0.6499114632606506f, text: 'Governance model: Feed is the single governance surface' }]"
*/

DEFINE ANALYZER test_analyzer TOKENIZERS blank, class, camel, punct FILTERS snowball(english), lowercase;
DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD text ON test TYPE string;
DEFINE INDEX idx_test_fulltext ON test FIELDS text FULLTEXT ANALYZER test_analyzer BM25(1.2, 0.75);

CREATE test:1 SET text = "IAM vision: Agents have identity and authority scopes";
CREATE test:2 SET text = "Governance model: Feed is the single governance surface";
CREATE test:3 SET text = "Target market is solo founders and tiny teams";

-- Direct query works
SELECT id, text, search::score(1) AS score FROM test WHERE text @1@ "governance" ORDER BY score DESC;

-- Inside a stored function should also work
DEFINE FUNCTION fn::test_search($query: string) {
  LET $rows = SELECT id, text, search::score(1) AS score
    FROM test
    WHERE text @1@ $query
    ORDER BY score DESC LIMIT 5;
  RETURN $rows;
};

RETURN fn::test_search("governance");
