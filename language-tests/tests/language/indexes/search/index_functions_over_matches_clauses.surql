/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A MATCHES clause and the index functions that report against it: each operator over analyzers that lowercase, stem and split into n-grams, a query the analyzer empties, several clauses told apart by their reference, score / highlight / offsets in projections and as ORDER BY keys, a clause the index does not serve judged per row beside OR and scalar filters, a clause the WHERE does not register, and computed fields over the rows."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'running dogs and cats', id: book:1, label: 'book 1', n: 1, name: 'Helloworld', tag: 'a', title: 'Alpha Beta guide' }]"

[[test.results]]
value = "[{ body: 'the dog runs home', id: book:2, label: 'book 2', n: 2, name: 'Goodbye', tag: 'b', title: 'Beta gamma notes' }]"

[[test.results]]
value = "[{ body: 'cats sleep all day', id: book:3, label: 'book 3', n: 3, name: 'Hellothere', tag: 'a', title: 'Alpha gamma notes' }]"

[[test.results]]
value = "[{ body: 'a dog and a cat', id: book:4, label: 'book 4', n: 4, name: 'Goodnight', tag: 'b', title: 'Delta guide' }]"

[[test.results]]
value = "[{ body: 'birds fly south', id: book:5, label: 'book 5', n: 5, name: 'Farewell', tag: 'a', title: 'Epsilon manual' }]"

[[test.results]]
value = "[{ body: 'fish swim all day', id: book:6, label: 'book 6', n: 6, name: 'Welcome', tag: 'b', title: 'Zeta manual' }]"

[[test.results]]
value = "[{ body: 'horses run fast', id: book:7, label: 'book 7', n: 7, name: 'Hellothere', tag: 'a', title: 'Eta almanac' }]"

[[test.results]]
value = "[{ id: book:1 }, { id: book:3 }]"

[[test.results]]
value = "[{ id: book:1 }, { id: book:4 }]"

[[test.results]]
value = "[{ id: book:1 }]"

[[test.results]]
value = "[{ id: book:2 }, { id: book:3 }, { id: book:4 }]"

[[test.results]]
value = "[{ id: book:1 }, { id: book:2 }, { id: book:7 }]"

[[test.results]]
value = "[{ id: book:1 }, { id: book:3 }, { id: book:5 }, { id: book:7 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: book:1, s: 0.7192269563674927f }, { id: book:3, s: 0.7192269563674927f }]"

[[test.results]]
value = "[{ id: book:2, s: 1.4384539127349854f }, { id: book:3, s: 1.4384539127349854f }]"

[[test.results]]
value = "[{ h: 'Beta gamma <b>notes</b>', id: book:2 }, { h: 'Alpha gamma <b>notes</b>', id: book:3 }]"

[[test.results]]
value = "[{ h: 'H<b>ell</b>oworld', id: book:1 }, { h: 'H<b>ell</b>othere', id: book:3 }, { h: 'Farew<b>ell</b>', id: book:5 }, { h: 'H<b>ell</b>othere', id: book:7 }]"

[[test.results]]
value = '[{ id: book:2, o: { "0": [{ e: 10, s: 5 }] } }, { id: book:3, o: { "0": [{ e: 11, s: 6 }] } }]'

[[test.results]]
value = '[{ id: book:1, o: { "0": [{ e: 4, s: 1 }] } }, { id: book:3, o: { "0": [{ e: 4, s: 1 }] } }, { id: book:5, o: { "0": [{ e: 8, s: 5 }] } }, { id: book:7, o: { "0": [{ e: 4, s: 1 }] } }]'

[[test.results]]
value = "[{ a: 0.7192269563674927f, b: 0.2475634664297104f, id: book:1 }, { a: 0.84980708360672f, b: 0.2241452932357788f, id: book:4 }]"

[[test.results]]
value = "[{ h: 'running dogs and [cats]', id: book:1 }, { h: 'a dog and a [cat]', id: book:4 }]"

[[test.results]]
value = "[{ id: book:1 }, { id: book:3 }, { id: book:4 }]"

[[test.results]]
value = "[{ id: book:1 }, { id: book:2 }, { id: book:3 }, { id: book:4 }]"

[[test.results]]
value = "[{ id: book:1 }, { id: book:4 }]"

[[test.results]]
value = "[{ id: book:1, s: 0f }, { id: book:2, s: 0.7192269563674927f }, { id: book:3, s: 0.7192269563674927f }]"

[[test.results]]
value = "[{ id: book:4 }]"

[[test.results]]
value = "[{ id: book:3 }]"

[[test.results]]
value = "[{ id: book:1, label: 'book 1', s: 0.7192269563674927f }, { id: book:3, label: 'book 3', s: 0.7192269563674927f }]"

[[test.results]]
value = "[{ id: book:1, inner: [0.2475634664297104f] }]"

[[test.results]]
error = "There was no suitable index supporting the expression: title @@ 'guide'"

[[test.results]]
error = "There was no suitable index supporting the expression: title @@ 'guide'"

[[test.results]]
value = "[{ id: book:1 }, { id: book:3 }]"

*/

DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase,ascii;
DEFINE ANALYZER stem TOKENIZERS blank,class FILTERS lowercase,snowball(english);
DEFINE ANALYZER grams TOKENIZERS blank FILTERS lowercase,ngram(2,3);
DEFINE TABLE book SCHEMALESS;
DEFINE FIELD label ON book COMPUTED 'book ' + <string> n;
DEFINE INDEX book_title ON book FIELDS title FULLTEXT ANALYZER simple BM25(1.2, 0.75) HIGHLIGHTS;
DEFINE INDEX book_body ON book FIELDS body FULLTEXT ANALYZER stem BM25(1.2, 0.75) HIGHLIGHTS;
DEFINE INDEX book_name ON book FIELDS name FULLTEXT ANALYZER grams BM25(1.2, 0.75) HIGHLIGHTS;
CREATE book:1 SET title = 'Alpha Beta guide', body = 'running dogs and cats', name = 'Helloworld', n = 1, tag = 'a';
CREATE book:2 SET title = 'Beta gamma notes', body = 'the dog runs home', name = 'Goodbye', n = 2, tag = 'b';
CREATE book:3 SET title = 'Alpha gamma notes', body = 'cats sleep all day', name = 'Hellothere', n = 3, tag = 'a';
CREATE book:4 SET title = 'Delta guide', body = 'a dog and a cat', name = 'Goodnight', n = 4, tag = 'b';
CREATE book:5 SET title = 'Epsilon manual', body = 'birds fly south', name = 'Farewell', n = 5, tag = 'a';
CREATE book:6 SET title = 'Zeta manual', body = 'fish swim all day', name = 'Welcome', n = 6, tag = 'b';
CREATE book:7 SET title = 'Eta almanac', body = 'horses run fast', name = 'Hellothere', n = 7, tag = 'a';

-- Each operator over each analyzer.
SELECT id FROM book WHERE title @1@ 'alpha' ORDER BY id;
SELECT id FROM book WHERE title @@ 'guide' ORDER BY id;
SELECT id FROM book WHERE title @AND@ 'alpha guide' ORDER BY id;
SELECT id FROM book WHERE title @OR@ 'delta notes' ORDER BY id;
SELECT id FROM book WHERE body @1@ 'run' ORDER BY id;
SELECT id FROM book WHERE name @1@ 'ell' ORDER BY id;
-- A query that is empty, and one the analyzer empties.
SELECT id FROM book WHERE title @1@ '' ORDER BY id;
SELECT id FROM book WHERE title @1@ '!!' ORDER BY id;
-- Score, highlight and offsets, in projections and as keys.
SELECT id, search::score(1) AS s FROM book WHERE title @1@ 'alpha' ORDER BY id;
SELECT id, search::score(1) AS s FROM book WHERE title @1@ 'gamma notes' ORDER BY s DESC, id;
SELECT id, search::highlight('<b>', '</b>', 1) AS h FROM book WHERE title @1@ 'notes' ORDER BY id;
SELECT id, search::highlight('<b>', '</b>', 1, true) AS h FROM book WHERE name @1@ 'ell' ORDER BY id;
SELECT id, search::offsets(1) AS o FROM book WHERE title @1@ 'gamma' ORDER BY id;
SELECT id, search::offsets(1, true) AS o FROM book WHERE name @1@ 'ell' ORDER BY id;
-- Several clauses, each with its own reference.
SELECT id, search::score(1) AS a, search::score(2) AS b FROM book WHERE title @1@ 'guide' AND body @2@ 'dog' ORDER BY id;
SELECT id, search::highlight('[', ']', 2) AS h FROM book WHERE title @1@ 'guide' AND body @2@ 'cat' ORDER BY id;
-- A clause no index serves, judged for each row.
SELECT id FROM book WHERE title @1@ 'alpha' OR n = 4 ORDER BY id;
SELECT id FROM book WHERE title @1@ 'alpha' OR body @2@ 'dog' ORDER BY id;
SELECT id FROM book WHERE title @1@ 'guide' AND (n = 1 OR body @2@ 'dog') ORDER BY id;
SELECT id, search::score(1) AS s FROM book WHERE title @1@ 'notes' OR n = 1 ORDER BY id;
-- A clause beside scalar filters, paging, counting and grouping.
SELECT id FROM book WHERE title @1@ 'guide' AND tag = 'b' ORDER BY id;
SELECT id FROM book WHERE title @1@ 'notes' ORDER BY id LIMIT 1 START 1;
-- Computed fields over the rows.
SELECT id, label, search::score(1) AS s FROM book WHERE title @1@ 'alpha' ORDER BY id;
-- A statement nested in one that declares a clause of its own.
SELECT id, (SELECT VALUE search::score(1) FROM book WHERE body @1@ 'dog' ORDER BY id LIMIT 1) AS inner FROM book:1;
-- A clause the WHERE does not register, and one with no WHERE.
SELECT id, title @@ 'guide' AS m FROM book ORDER BY id;
SELECT id FROM book WHERE array::len([title @@ 'guide']) > 0;
-- Rows a record id names are judged on their own value.
SELECT id FROM book:1..=3 WHERE title @@ 'alpha';
