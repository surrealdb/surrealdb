/**
[env]
# Extra timeout because this test sleeps to wait for the deferred daemon.
timeout = 5000

[test]
issue = 7234
reason = "Deferred SEARCH index appends must re-resolve the analyzer after the initial build, so DEFINE ANALYZER OVERWRITE changes are observed by both indexing and querying."

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
value = "NONE"

[[test.results]]
value = "[{ id: article:1 }]"

[[test.results]]
match = """
$result.building.status == 'ready'
"""
*/

DEFINE ANALYZER deferredSearch TOKENIZERS blank;
DEFINE INDEX search_title ON article FIELDS title SEARCH ANALYZER deferredSearch BM25(1.2, 0.75) CONCURRENTLY DEFER;
SLEEP 1s;

DEFINE ANALYZER OVERWRITE deferredSearch TOKENIZERS blank FILTERS lowercase;
CREATE article:1 CONTENT { title: 'HELLO' } RETURN NONE;
SLEEP 1s;

SELECT id FROM article WHERE title @@ 'hello';
INFO FOR INDEX search_title ON article;
