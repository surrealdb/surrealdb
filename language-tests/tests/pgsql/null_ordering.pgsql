/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL NULLS FIRST and NULLS LAST ordering semantics"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, score: 100i32 }, { id: 2, score: NULL }, { id: 3, score: 50i32 }]"

[[test.results]]
value = "[{ id: 2 }, { id: 3 }, { id: 1 }]"

[[test.results]]
value = "[{ id: 3 }, { id: 1 }, { id: 2 }]"
*/

CREATE TABLE scores (id INT, score INT);
INSERT INTO scores (id, score) VALUES (1, 100), (2, NULL), (3, 50);
SELECT s.id FROM scores s ORDER BY s.score ASC NULLS FIRST;
SELECT s.id FROM scores s ORDER BY s.score ASC NULLS LAST;
