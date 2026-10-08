/**
[env]
namespace = true
database = true
imports = ["access/record/matches_select_permission_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = "A full-text clause, the index functions that report against it and a vector search's distances answer for a record user as the table's SELECT permission lets them: bob's notes are neither returned nor scored nor highlighted, a field the user may not read is not returned beside a score, a clause judged per row beside OR sees only the rows the permission admits, a denied table yields no row and a count over it zero, and a nearest-neighbour search returns the k nearest rows the permission admits, so a row the user may not see takes none of the k."

[[test.results]]
value = "user:alice"

[[test.results]]
value = "[{ id: note:1, s: 0f }, { id: note:3, s: 0f }, { id: note:5, s: 0f }]"

[[test.results]]
value = "[{ h: 'red apple <b>pie</b>', id: note:1, owner: user:alice, point: [10f], text: 'red apple pie' }, { h: 'green apple <b>pie</b>', id: note:3, owner: user:alice, point: [30f], text: 'green apple pie' }]"

[[test.results]]
value = '[{ id: note:1, o: { "0": [{ e: 3, s: 0 }] } }, { id: note:5, o: { "0": [{ e: 3, s: 0 }] } }]'

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: note:1, s: 0f }, { id: note:3, s: 0f }, { id: note:5, s: 0f }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ d: 10f, id: note:1 }, { d: 30f, id: note:3 }, { d: 50f, id: note:5 }]"

[[test.results]]
value = "[{ d: 10f, id: note:1 }, { d: 30f, id: note:3 }, { d: 50f, id: note:5 }]"

*/

RETURN $auth;
SELECT id, search::score(1) AS s FROM note WHERE text @1@ 'apple' ORDER BY id;
SELECT *, search::highlight('<b>', '</b>', 1) AS h FROM note WHERE text @1@ 'pie' ORDER BY id;
SELECT id, search::offsets(1) AS o FROM note WHERE text @1@ 'red' ORDER BY id;
SELECT id FROM note WHERE text @1@ 'tart' OR text @2@ 'cherry' ORDER BY id;
SELECT id, search::score(1) AS s FROM note WHERE text @1@ 'apple' OR id = note:6 ORDER BY id;
SELECT count() FROM note WHERE text @1@ 'apple' GROUP ALL;
SELECT id, search::score(1) AS s FROM sealed WHERE text @1@ 'apple' ORDER BY id;
SELECT count() FROM sealed WHERE text @1@ 'apple' GROUP ALL;
SELECT id, vector::distance::knn() AS d FROM note WHERE point <|3,40|> [0f] ORDER BY id;
SELECT id, vector::distance::knn() AS d FROM note WHERE point <|6,40|> [0f] ORDER BY d;
