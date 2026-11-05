/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An UPDATE or DELETE whose targets mix a table or record id with a subquery, a parameter or an object writes each target in written order, and a value a classified target yields names the stored record rather than standing in for it"
issue = 1712

[[test.results]]
value = "[{ a: 1, id: nn:1 }]"

[[test.results]]
value = "[{ a: 2, id: nn:2 }]"

[[test.results]]
value = "[{ id: nn:1, n: 1 }, { id: nn:2, n: 1 }, { id: nn:1, n: 2 }]"

[[test.results]]
value = "[{ id: nn:1 }, { id: nn:2 }]"

[[test.results]]
value = "[{ a: 1, id: nn:1, o: true }, { a: 2, id: nn:2, o: true }, { a: 2, id: nn:2, o: true }]"

[[test.results]]
value = "[{ id: nn:1 }, { id: nn:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: nn:1, p: true }, { a: 2, id: nn:2, p: true }, { a: 1, id: nn:1, p: true }]"

[[test.results]]
value = "[{ a: 1, id: nn:1, q: true }, { a: 2, id: nn:2, q: true }, { a: 2, id: nn:2, q: true }]"

[[test.results]]
value = "[{ a: 1, id: nn:1, r: true }, { a: 2, id: nn:2, r: true }]"

[[test.results]]
value = "[{ g: true, id: nn:1 }, { g: true, id: nn:2 }]"

[[test.results]]
value = "[{ id: nn:1, m: 1 }, { id: nn:1, m: 2 }]"

[[test.results]]
value = "[{ id: nn:2 }]"

[[test.results]]
value = "[{ a: 1, e: true, g: true, id: nn:1, m: 2, n: 2, o: true, p: true, q: true, r: true, touched: true }, { a: 2, e: true, g: true, id: nn:2, n: 1, o: true, p: true, q: true, r: true, touched: true, w: true }]"

[[test.results]]
value = "[{ a: 1, e: true, g: true, id: nn:1, m: 2, n: 2, o: true, p: true, q: true, r: true, touched: true }, { a: 2, e: true, g: true, id: nn:2, n: 1, o: true, p: true, q: true, r: true, touched: true, w: true }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 3, id: nn:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 3, id: nn:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'hello world', id: doc:1, y: 1 }]"

[[test.results]]
value = "[{ body: 'bye', id: doc:2, y: 2 }]"

[[test.results]]
value = "[doc:2, doc:2]"

[[test.results]]
value = "[doc:1, doc:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: doc:1, n: 1, y: 1 }, { id: doc:2, n: 1, y: 2 }, { id: doc:1, n: 2, y: 1 }, { id: doc:2, n: 2, y: 2 }, { id: doc:2, n: 3, y: 2 }]"

[[test.results]]
value = "[{ id: doc:1, u: true, y: 1 }, { id: doc:2, u: true, y: 2 }, { id: doc:3, u: true, y: NONE }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:1, keep: 1, point: [0, 0] }]"

[[test.results]]
value = "[{ id: pts:2, keep: 2, point: [5, 5] }]"

[[test.results]]
value = "[{ id: pts:3, keep: 3, point: [9, 9] }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: A nearest-neighbour (KNN) condition cannot be used on a write whose targets mix a table or record with a subquery, parameter or value. Write those targets in separate statements instead."

[[test.results]]
error = "Invalid query: A nearest-neighbour (KNN) condition cannot be used on a write whose targets mix a table or record with a subquery, parameter or value. Write those targets in separate statements instead."

[[test.results]]
error = "Invalid query: A nearest-neighbour (KNN) condition cannot be used on a write whose targets mix a table or record with a subquery, parameter or value. Write those targets in separate statements instead."

[[test.results]]
error = "Invalid query: A nearest-neighbour (KNN) condition cannot be used on a write whose targets mix a table or record with a subquery, parameter or value. Write those targets in separate statements instead."

[[test.results]]
error = "Invalid query: A nearest-neighbour (KNN) condition cannot be used on a write whose targets mix a table or record with a subquery, parameter or value. Write those targets in separate statements instead."

[[test.results]]
value = "[{ id: pts:1, keep: 1, point: [0, 0] }, { id: pts:2, keep: 2, point: [5, 5] }, { id: pts:3, keep: 3, point: [9, 9] }]"

[[test.results]]
value = "[pts:1]"
*/

CREATE nn:1 SET a = 1;
CREATE nn:2 SET a = 2;
-- A subquery naming a record the table already wrote writes it again.
UPDATE nn, (SELECT VALUE nn:1 FROM ONLY nn:1) SET n += 1 RETURN id, n;
-- A subquery naming a record no branch holds writes nothing there.
UPDATE nn, (SELECT VALUE nn:9 FROM ONLY nn:1) SET touched = true RETURN id;
-- A subquery's objects name the stored record: the fields the projection
-- dropped are kept.
UPDATE nn, (SELECT id FROM nn WHERE id = nn:2) SET o = true RETURN id, a, o;
-- A subquery yielding nothing adds no subject.
UPDATE nn, (SELECT VALUE id FROM nn WHERE false) SET e = true RETURN id;
-- A parameter's or an object's value names the stored record too.
LET $first = { id: nn:1 };
UPDATE nn, $first SET p = true RETURN id, a, p;
UPDATE nn, { id: nn:2 } SET q = true RETURN id, a, q;
-- A record id beside a subquery.
UPDATE nn:1, (SELECT VALUE nn:2 FROM ONLY nn:1) SET r = true RETURN id, a, r;
-- A record range or a mock beside a subquery.
UPDATE nn:1..=1, (SELECT VALUE nn:2 FROM ONLY nn:1) SET g = true RETURN id, g;
UPDATE |nn:1..2|, (SELECT VALUE nn:1 FROM ONLY nn:1) SET m += 1 RETURN id, m;
-- The condition judges the subjects of every target.
UPDATE nn, (SELECT VALUE id FROM nn WHERE id = nn:1) SET w = true WHERE a = 2 RETURN id;
SELECT * FROM nn ORDER BY id;
-- A record the table branch deleted is not there for the subquery to delete.
DELETE nn, (SELECT VALUE id FROM nn LIMIT 1) RETURN BEFORE;
SELECT * FROM nn;
CREATE nn:3 SET a = 3;
LET $third = { id: nn:3 };
DELETE nn, $third RETURN BEFORE;
SELECT * FROM nn;
-- A table's indexes beside a subquery: the condition judges every subject,
-- whichever target named it.
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX body ON doc FIELDS body FULLTEXT ANALYZER simple BM25;
DEFINE INDEX y ON doc FIELDS y;
CREATE doc:1 SET body = 'hello world', y = 1;
CREATE doc:2 SET body = 'bye', y = 2;
UPDATE doc, (SELECT VALUE doc:2 FROM ONLY doc:1) SET i = true WHERE y = 2 RETURN VALUE id;
UPDATE doc, (SELECT VALUE doc:1 FROM ONLY doc:2) SET f = true WHERE body @@ 'hello' RETURN VALUE id;
-- Three and more targets of every kind, each written in order.
LET $second = { id: doc:2 };
UPDATE doc, doc:1, (SELECT VALUE doc:2 FROM ONLY doc:1), $second SET n += 1 RETURN id, n, y;
-- UPSERT names its targets whatever they are: a subquery's or an object's
-- record is the stored one, or the one it creates.
UPSERT doc:1, (SELECT VALUE doc:2 FROM ONLY doc:1), { id: doc:3 } SET u = true RETURN id, u, y;
-- No search answers a nearest-neighbour condition over a list mixing a table
-- with a subquery, a parameter or a value, so the statement is refused rather
-- than skipping the nearest records or writing a value as a record.
DEFINE INDEX near ON pts FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN;
CREATE pts:1 SET point = [0, 0], keep = 1;
CREATE pts:2 SET point = [5, 5], keep = 2;
CREATE pts:3 SET point = [9, 9], keep = 3;
LET $near = { id: pts:1, point: [0, 0] };
UPDATE pts, $near SET k = true WHERE point <|3,EUCLIDEAN|> [0, 0] RETURN AFTER;
UPDATE pts, { id: pts:1, point: [0, 0] } SET k = true WHERE point <|3,EUCLIDEAN|> [0, 0] RETURN AFTER;
UPDATE pts, (SELECT VALUE pts:2 FROM ONLY 1) SET k = true WHERE point <|2,EUCLIDEAN|> [0, 0] RETURN VALUE id;
UPDATE pts, (SELECT VALUE pts:2 FROM ONLY 1) SET k = true WHERE point <|1,40|> [0, 0] RETURN VALUE id;
DELETE pts, (SELECT VALUE pts:2 FROM ONLY 1) WHERE point <|1,40|> [0, 0] RETURN BEFORE;
SELECT * FROM pts ORDER BY id;
-- Each target alone is ranked as ever.
UPDATE pts SET k = true WHERE point <|1,EUCLIDEAN|> [0, 0] RETURN VALUE id;
