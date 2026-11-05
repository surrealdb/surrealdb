/**
[env]
namespace = true
database = true
imports = ["pgsql/_id_key.surql"]

[test]
reason = "A PostgreSQL comparison of id with an integer literal, or an IN list of that one integer, reads the record by its id instead of scanning the table: the table has a computed field that raises on the row with key 2, so a statement that reads that row raises, and the lookups of the other rows, alone or with other conditions, never read it. A range of ids and a condition on another column scan the table, and read it."

[[test.results]]
error = "An error occurred: the row with key 2 was read"

[[test.results]]
value = "[{ id: 1, trap: 0 }]"

[[test.results]]
value = "[{ id: 3, trap: 0 }]"

[[test.results]]
value = "[{ id: 3, trap: 0 }]"

[[test.results]]
value = "[{ id: 1, trap: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
error = "An error occurred: the row with key 2 was read"

[[test.results]]
value = "[{ id: 1, n: 10 }]"

[[test.results]]
error = "An error occurred: the row with key 2 was read"

[[test.results]]
value = "[{ id: 3 }]"

[[test.results]]
error = "An error occurred: the row with key 2 was read"

*/

SELECT id, trap FROM guarded ORDER BY id;

SELECT id, trap FROM guarded WHERE id = 1;

SELECT id, trap FROM guarded WHERE 3 = id;

SELECT id, trap FROM guarded WHERE id IN (3);

SELECT id, trap FROM guarded WHERE id = 1 AND n = 1;

SELECT id, trap FROM guarded WHERE n = 1 AND id = 3;

SELECT id, trap FROM guarded WHERE id > 2;

UPDATE guarded SET n = 10 WHERE id = 1 RETURNING id, n;

UPDATE guarded SET n = 11 WHERE n > 0 RETURNING id, n;

DELETE FROM guarded WHERE id = 3 RETURNING id;

DELETE FROM guarded WHERE id > 0 RETURNING id;
