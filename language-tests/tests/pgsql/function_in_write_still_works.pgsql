/**
[env]
namespace = true
database = true

[test]
reason = "A function the engine function answers still works in the expressions a write lowers, and answers NULL for a NULL argument"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, n: -7i32, s: 'HELLO' }, { id: 2, n: 3i32, s: NULL }]"

[[test.results]]
value = "[{ id: 1, n: 7i32, s: 'hello' }, { id: 2, n: 3i32, s: NULL }]"

[[test.results]]
value = "[{ id: 1, n: 7i32, s: 'hello' }, { id: 2, n: 3i32, s: NULL }]"

*/

CREATE TABLE wr (id int PRIMARY KEY, s text, n int);

INSERT INTO wr (id, s, n) VALUES (1, 'HELLO', -7), (2, NULL, 3);

UPDATE wr SET s = lower(s), n = abs(n);

SELECT id, s, n FROM wr ORDER BY id;
