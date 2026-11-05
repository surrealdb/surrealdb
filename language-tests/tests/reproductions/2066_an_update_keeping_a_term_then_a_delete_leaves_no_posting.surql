/**
[env]
namespace = true
database = true

[test]
issue = 2066
reason = """
A document updated with text that keeps a term, and then deleted, carries no
term afterwards: the update's removal and re-addition of the kept term leave
its membership as it was, so the delete takes it out.

The rows a full-text query returns cannot show a stale membership, because a
deleted document's id resolves to no record. What does show it is the term's
document count, which BM25 weighs: `d` ends holding what the control table `c`
holds, so a query over either scores its one match the same.

The test datastore runs no compaction, so this covers the uncompacted changes.
Compaction of the same sequence is pinned in the full-text index's own tests.
"""

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[d:2]"

[[test.results]]
value = "true"
*/

DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25;
DEFINE INDEX c_body ON c FIELDS body FULLTEXT ANALYZER simple BM25;

-- The control: what `d` holds once its first document is gone.
CREATE c:2 SET body = 'hello' RETURN NONE;
CREATE c:3 SET body = 'other words' RETURN NONE;
CREATE c:4 SET body = 'more words' RETURN NONE;
CREATE c:5 SET body = 'still more' RETURN NONE;

CREATE d:2 SET body = 'hello' RETURN NONE;
CREATE d:3 SET body = 'other words' RETURN NONE;
CREATE d:4 SET body = 'more words' RETURN NONE;
CREATE d:5 SET body = 'still more' RETURN NONE;

-- `hello` is kept by the update, `world` dropped and `there` added.
CREATE d:1 SET body = 'hello world' RETURN NONE;
UPDATE d:1 SET body = 'hello there' RETURN NONE;
DELETE d:1;

SELECT count() FROM d WHERE body @@ 'hello' GROUP ALL;
SELECT count() FROM d WITH NOINDEX WHERE body @@ 'hello' GROUP ALL;
SELECT VALUE id FROM d WHERE body @@ 'hello' OR body @@ 'there' OR body @@ 'world';

-- The kept term counts one document, as it does in the control.
(SELECT VALUE search::score(1) FROM d WHERE body @1@ 'hello')
	== (SELECT VALUE search::score(1) FROM c WHERE body @1@ 'hello');
