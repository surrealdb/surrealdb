/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A full-text match against a parameter filters like the literal and search::score finds its MATCHES clause"
issue = 7014

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Governance model for the feed']"

[[test.results]]
value = "[{ scored: true, text: 'Governance model for the feed' }]"

[[test.results]]
value = "['Governance model for the feed']"
*/

DEFINE ANALYZER test_analyzer TOKENIZERS blank, class, camel, punct FILTERS snowball(english), lowercase;
DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD text ON test TYPE string;
DEFINE INDEX idx_test_fulltext ON test FIELDS text FULLTEXT ANALYZER test_analyzer BM25(1.2, 0.75);
CREATE test:1 SET text = "IAM vision for agents" RETURN NONE;
CREATE test:2 SET text = "Governance model for the feed" RETURN NONE;
CREATE test:3 SET text = "Target market is solo founders" RETURN NONE;
LET $query = "governance";
SELECT VALUE text FROM test WHERE text @1@ $query;
SELECT text, search::score(1) > 0 AS scored FROM test WHERE text @1@ $query;
SELECT VALUE text FROM test WHERE text @0@ $query;
