/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Predicates that share a MATCHES reference: search::highlight and search::offsets cover the terms of all of them, including for a row that matches only one predicate."

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

[[test.results]]
value = "[{ h: 'lazy <dog> and <cat>', id: d:1 }]"

[[test.results]]
value = "[{ h: 'lazy <dog> and cat', id: d:1 }, { h: '<dog> only', id: d:2 }]"

[[test.results]]
value = "[{ body: 'only a bird', id: d:2 }]"

[[test.results]]
value = "[{ h: 'lazy <dog> and cat', id: d:1 }, { h: 'only a <bird>', id: d:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ h: 'a <cat> here', id: d:3 }]"
*/

DEFINE ANALYZER ft TOKENIZERS blank FILTERS lowercase;
DEFINE TABLE d SCHEMAFULL;
DEFINE FIELD body ON d TYPE string;
CREATE d:1 SET body = 'lazy dog and cat';
DEFINE INDEX ib ON d FIELDS body FULLTEXT ANALYZER ft BM25 HIGHLIGHTS;
CREATE d:2 SET body = 'dog only';
CREATE d:3 SET body = 'a cat here';
SELECT id, search::highlight('<', '>', 1) AS h FROM d WHERE body @1@ 'dog' OR body @1@ 'cat' ORDER BY id;
SELECT id, search::offsets(1) AS o FROM d WHERE body @1@ 'dog' OR body @1@ 'cat' ORDER BY id;
SELECT id, search::highlight('<', '>', 1) AS h FROM d WHERE body @1@ 'dog' AND body @1@ 'cat' ORDER BY id;
SELECT id, search::highlight('<', '>', 1) AS h FROM d WHERE body @1@ 'dog' ORDER BY id;
UPDATE d:2 SET body = 'only a bird';
SELECT id, search::highlight('<', '>', 1) AS h FROM d WHERE body @1@ 'dog' OR body @1@ 'bird' ORDER BY id;
DELETE d:1;
SELECT id, search::highlight('<', '>', 1) AS h FROM d WHERE body @1@ 'dog' OR body @1@ 'cat' ORDER BY id;
