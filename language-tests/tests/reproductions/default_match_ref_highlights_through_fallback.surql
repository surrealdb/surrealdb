/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "MATCHES predicates written without a reference all carry the default one: search::highlight and search::offsets naming another reference fall back to them and cover the terms of every predicate."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'lazy dog and cat', id: d:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'dog only', id: d:2 }]"

[[test.results]]
value = "[{ body: 'a cat here', id: d:3 }]"

[[test.results]]
value = "[{ h: 'lazy <dog> and <cat>', id: d:1 }, { h: '<dog> only', id: d:2 }, { h: 'a <cat> here', id: d:3 }]"

[[test.results]]
value = '[{ id: d:1, o: { "0": [{ e: 8, s: 5 }, { e: 16, s: 13 }] } }, { id: d:2, o: { "0": [{ e: 3, s: 0 }] } }, { id: d:3, o: { "0": [{ e: 5, s: 2 }] } }]'
*/

DEFINE ANALYZER ft TOKENIZERS blank FILTERS lowercase;
DEFINE TABLE d SCHEMAFULL;
DEFINE FIELD body ON d TYPE string;
CREATE d:1 SET body = 'lazy dog and cat';
DEFINE INDEX ib ON d FIELDS body FULLTEXT ANALYZER ft BM25 HIGHLIGHTS;
CREATE d:2 SET body = 'dog only';
CREATE d:3 SET body = 'a cat here';
SELECT id, search::highlight('<', '>', 1) AS h FROM d WHERE body @@ 'dog' OR body @@ 'cat' ORDER BY id;
SELECT id, search::offsets(1) AS o FROM d WHERE body @@ 'dog' OR body @@ 'cat' ORDER BY id;
