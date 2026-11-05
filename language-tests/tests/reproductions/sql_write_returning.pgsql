/**
[env]
namespace = true
database = true
imports = ["reproductions/sql_write_returning_import.surql"]

[test]
reason = "Write casts convert their inputs, and RETURNING expressions retain their requested labels."

[[test.results]]
value = "[{ id: 1, age: 42, name: 'Ada' }]"
[[test.results]]
value = "[{ next_age: 44, person: 'Ada' }]"
[[test.results]]
value = "[{ key: 1, person: 'Ada' }]"
*/
INSERT INTO write_returning (id, age, name) VALUES (1, '42'::int, 'Ada') RETURNING name, id, age;
UPDATE write_returning SET age = '43'::int WHERE id = 1 RETURNING name AS person, age + 1 AS next_age;
DELETE FROM write_returning WHERE id = 1 RETURNING name AS person, id AS key;
