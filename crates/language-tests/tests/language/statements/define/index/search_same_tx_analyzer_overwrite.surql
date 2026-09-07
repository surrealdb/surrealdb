/**
[env]
timeout = 4000

[test]
issue = 7234
reason = "A SEARCH index defined in the same transaction as DEFINE ANALYZER OVERWRITE must use the uncommitted overwritten analyzer, even when a background build transaction can still see the previous committed analyzer."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: article:1 }]"

[[test.results]]
match = """
$result.building.status == 'ready'
"""
*/

DEFINE ANALYZER sameTxOverwriteSearch TOKENIZERS blank;
CREATE article:1 CONTENT { title: 'HELLO' } RETURN NONE;

BEGIN;
DEFINE ANALYZER OVERWRITE sameTxOverwriteSearch TOKENIZERS blank FILTERS lowercase;
DEFINE INDEX search_title ON article FIELDS title SEARCH ANALYZER sameTxOverwriteSearch BM25(1.2, 0.75);
COMMIT;

SELECT id FROM article WHERE title @@ 'hello';
INFO FOR INDEX search_title ON article;
