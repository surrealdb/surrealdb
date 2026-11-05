/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A query word longer than an ngram analyzer's max matches the documents that hold all of its windows; a word within [min, max] matches as itself; a word shorter than min matches nothing. Rows written before and after the index is defined, and rows updated or deleted, are all found. search::highlight marks the windows of a query word a row holds, even when it holds only some of them, while such a row scores zero."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'abcdefgh', id: q:1 }]"

[[test.results]]
value = "[{ body: 'xbcdyz', id: q:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'abcdefgh', id: q:3 }]"

[[test.results]]
value = "[{ id: q:1 }, { id: q:3 }]"

[[test.results]]
value = "[{ id: q:1 }, { id: q:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: q:1 }, { id: q:3 }]"

[[test.results]]
value = "[{ body: 'zzzz', id: q:1 }]"

[[test.results]]
value = "[{ id: q:3 }]"

[[test.results]]
value = "[{ id: q:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: q:2 }]"

[[test.results]]
value = "[{ h: '<xbcdyz>', id: q:2, s: 0f }]"
*/

DEFINE ANALYZER ng TOKENIZERS blank FILTERS lowercase, ngram(2,2);
DEFINE TABLE q SCHEMAFULL;
DEFINE FIELD body ON q TYPE string;
CREATE q:1 SET body = 'abcdefgh';
CREATE q:2 SET body = 'xbcdyz';
DEFINE INDEX iq ON q FIELDS body FULLTEXT ANALYZER ng BM25 HIGHLIGHTS;
CREATE q:3 SET body = 'abcdefgh';
SELECT id FROM q WHERE body @1@ 'abc' ORDER BY id;
SELECT id FROM q WHERE body @1@ 'ab bc' ORDER BY id;
SELECT id FROM q WHERE body @1@ 'abcx' ORDER BY id;
SELECT id FROM q WHERE body @1@ 'a' ORDER BY id;
SELECT id FROM q WHERE body @1@ 'ab' ORDER BY id;
UPDATE q:1 SET body = 'zzzz';
SELECT id FROM q WHERE body @1@ 'abc' ORDER BY id;
SELECT id FROM q WHERE body @1@ 'zz' ORDER BY id;
DELETE q:3;
SELECT id FROM q WHERE body @1@ 'bcd' ORDER BY id;
SELECT id, search::highlight('<', '>', 1) AS h, search::score(1) AS s FROM q WHERE body @1@ 'abcx' OR id = q:2 ORDER BY id;
