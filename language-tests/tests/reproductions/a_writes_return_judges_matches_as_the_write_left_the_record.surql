/**
[env]
namespace = true
database = true

[test]
reason = "A write's RETURN clause projects each record as the write left it, and judges a MATCHES the WHERE registers, or a search::highlight, search::offsets or search::score call bound to it, against that record: a record that kept or gained the query's terms matches and is highlighted and scored, one that lost them, or that the write deleted, does not, whether the terms were read before the statement wrote the record or after an earlier statement of its transaction did. A MATCHES in RETURN that the WHERE does not register is refused, as one in SET is. A search call of a row written after the statement removed the index finds no index, though an earlier row's call resolved it."

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
value = "[{ body: 'hello', id: doc:1, n: 1 }]"

[[test.results]]
value = "[{ body: 'hello', id: doc:2, n: 2 }]"

[[test.results]]
value = "[{ body: 'other', id: doc:3, n: 3 }]"

[[test.results]]
value = "[{ id: doc:1, m: true }, { id: doc:2, m: true }]"

[[test.results]]
value = "[{ body: 'bye', id: doc:1, still: false }, { body: 'bye', id: doc:2, still: false }]"

[[test.results]]
value = "[{ id: doc:3, m: true }]"

[[test.results]]
value = "[{ id: doc:1, m: true }, { id: doc:3, m: true }]"

[[test.results]]
error = "There was no suitable index supporting the expression: body @@ 'there'"

[[test.results]]
value = "[true]"

[[test.results]]
value = "[false]"

[[test.results]]
value = "[{ id: doc:1, m: true }, { id: doc:2, m: true }, { id: doc:3, m: true }]"

[[test.results]]
value = "[{ id: doc:1, m: false }, { id: doc:3, m: false }]"

[[test.results]]
value = "[{ id: NONE, m: false }]"

[[test.results]]
value = "[{ body: 'hello', id: two:1 }]"

[[test.results]]
value = "[{ body: 'hello there', id: two:2 }]"

[[test.results]]
value = "[{ id: two:1, m: true }, { id: two:2, m: false }]"

[[test.results]]
value = "[{ body: 'hello', id: hl:1, n: 1 }]"

[[test.results]]
value = "[{ body: 'other', id: hl:2, n: 2 }]"

[[test.results]]
value = """[{ h: '<b>hello</b> <b>hello</b>', id: hl:1, o: { "0": [{ e: 5, s: 0 }, { e: 11, s: 6 }] }, scored: true }, { h: '<b>hello</b> <b>hello</b>', id: hl:2, o: { "0": [{ e: 5, s: 0 }, { e: 11, s: 6 }] }, scored: true }]"""

[[test.results]]
value = "[{ h: 'other', id: hl:1, o: NONE, s: 0f }, { h: 'other', id: hl:2, o: NONE, s: 0f }]"

[[test.results]]
value = "[{ body: 'bye', id: tx:1 }]"

[[test.results]]
value = "[{ body: 'bye', id: tx:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: tx:1, m: false }, { id: tx:2, m: false }]"

[[test.results]]
value = "[{ id: tx:1, m: true }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[tx:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'hello', id: gone:1, n: 1 }]"

[[test.results]]
value = "[{ body: 'hello', id: gone:2, n: 2 }]"

[[test.results]]
value = "[{ h: '<b>hello</b>', id: gone:1, scored: true }, { h: NONE, id: gone:2, scored: false }]"
*/
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft ON doc FIELDS body FULLTEXT ANALYZER simple BM25;
DEFINE INDEX ft ON two FIELDS body FULLTEXT ANALYZER simple BM25;
DEFINE INDEX ft ON hl FIELDS body FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
DEFINE INDEX ft ON tx FIELDS body FULLTEXT ANALYZER simple BM25;
CREATE doc:1 SET body = 'hello', n = 1;
CREATE doc:2 SET body = 'hello', n = 2;
CREATE doc:3 SET body = 'other', n = 3;
UPDATE doc SET n = n + 10 WHERE body @@ 'hello' RETURN id, body @@ 'hello' AS m;
UPDATE doc SET body = 'bye' WHERE body @@ 'hello' RETURN id, body, body @@ 'hello' AS still;
UPDATE doc SET body = 'hello' WHERE body @@ 'hello' OR n = 3 RETURN id, body @@ 'hello' AS m;
UPDATE doc WITH NOINDEX SET body = 'hello there' WHERE body @@ 'hello' OR n = 11 RETURN id, body @@ 'hello' AS m;
UPDATE doc SET n = n WHERE n > 0 RETURN body @@ 'there' AS m;
UPDATE doc:2 SET n = 22 WHERE body @@ 'bye' RETURN VALUE body @@ 'bye';
UPDATE doc:2 SET body = 'there' WHERE body @@ 'bye' RETURN VALUE body @@ 'bye';
UPSERT doc SET n = n + 1 WHERE body @@ 'there' RETURN id, body @@ 'there' AS m;
UPSERT doc SET body = 'bye' WHERE body @@ 'hello' RETURN id, body @@ 'hello' AS m;
DELETE doc WHERE body @@ 'there' RETURN id, body @@ 'there' AS m;
CREATE two:1 SET body = 'hello';
CREATE two:2 SET body = 'hello there';
UPDATE two SET body = IF id = two:1 THEN 'hello there' ELSE 'hello' END WHERE body @@ 'hello there' OR id = two:1 RETURN id, body @@ 'hello there' AS m;
CREATE hl:1 SET body = 'hello', n = 1;
CREATE hl:2 SET body = 'other', n = 2;
UPDATE hl SET body = 'hello hello' WHERE body @1@ 'hello' OR n = 2 RETURN id, search::highlight('<b>', '</b>', 1) AS h, search::offsets(1) AS o, search::score(1) > 0 AS scored;
UPDATE hl SET body = 'other' WHERE body @1@ 'hello' RETURN id, search::highlight('<b>', '</b>', 1) AS h, search::offsets(1) AS o, search::score(1) AS s;
CREATE tx:1 SET body = 'bye';
CREATE tx:2 SET body = 'bye';
BEGIN;
UPDATE tx SET body = 'hello' RETURN NONE;
UPDATE tx SET body = 'bye' WHERE body @@ 'hello' RETURN id, body @@ 'hello' AS m;
UPDATE tx SET body = 'hello' WHERE body @@ 'hello' OR id = tx:1 RETURN id, body @@ 'hello' AS m;
COMMIT;
SELECT VALUE id FROM tx WHERE body @@ 'hello';
DEFINE INDEX ft ON gone FIELDS body FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
DEFINE FUNCTION fn::remove_gone() { REMOVE INDEX ft ON gone; RETURN true; };
CREATE gone:1 SET body = 'hello', n = 1;
CREATE gone:2 SET body = 'hello', n = 2;
UPDATE gone SET removed = n = 2 AND fn::remove_gone() WHERE body @1@ 'hello' RETURN id, search::score(1) > 0 AS scored, search::highlight('<b>', '</b>', 1) AS h;
