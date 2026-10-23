/**
[env]
namespace = true
database = true
imports = ["pgsql/_id_key.surql"]

[test]
reason = "UPDATE and DELETE of a PostgreSQL table by its id column change exactly the rows whose key matches, and report them under the key; INSERT reports the key it wrote, ON CONFLICT (id) finds the row of the key, and assigning to id is refused. A column that holds the key or a link to the row, compared with the id, finds the same rows."

[[test.results]]
value = "[{ age: 43, id: 1, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: 2 }, { id: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 5, id: 10 }]"

[[test.results]]
value = "[{ age: 1000, id: 10 }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 1, id: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 43, id: 1, name: 'x' }, { age: 0, id: 2, name: 'Jaime' }, { age: 1, id: 3, name: 'Zed' }, { age: 1000, id: 10, name: 'Ten' }]"

[[test.results]]
value = "[{ id: 'rust', label: 'Rust!' }]"

[[test.results]]
value = "[{ id: 'person:1', label: 'Spelled' }]"

[[test.results]]
value = "[{ id: '1', label: 'One' }]"

[[test.results]]
value = "[{ id: 1, name: 'x' }, { id: 2, name: 'Jaime' }, { id: 3, name: 'Zed' }, { id: 10, name: 'Ten' }]"

[[test.results]]
value = "[{ age: 7, id: 1, name: 'x' }, { age: 7, id: 2, name: 'Jaime' }, { age: 7, id: 3, name: 'Zed' }, { age: 7, id: 10, name: 'Ten' }]"

[[test.results]]
value = "[{ id: 2, name: 'Jaime' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: 1 }, { id: 3 }, { id: 10 }]"

[[test.results]]
value = "[{ age: 7, id: 1, name: 'x' }, { age: 7, id: 3, name: 'Zed' }]"

[[test.results]]
value = "[{ id: 10 }]"

[[test.results]]
value = "[{ id: 2 }]"

[[test.results]]
value = "[{ age: 1, id: 2, name: 'Back' }]"

[[test.results]]
value = "[{ age: 7, id: 10, name: 'Ten' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: 'db' }]"

[[test.results]]
value = "[{ id: '1', label: 'One' }, { id: 'person:1', label: 'Spelled' }, { id: 'rust', label: 'Rust!' }]"

[[test.results]]
value = "[{ id: 20, name: 'New' }]"

[[test.results]]
value = "[{ id: 20, name: 'New' }]"

[[test.results]]
value = "[{ age: 2, id: 20, name: 'Again' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: 21 }]"

[[test.results]]
value = "[{ age: 2, id: 20, name: 'Again' }, { age: 3, id: 21, name: 'Other' }]"

[[test.results]]
value = "[{ id: 'new', label: 'New tag' }]"

[[test.results]]
value = "[{ id: 'new', label: 'New tag' }]"

[[test.results]]
error = "Found 5 for the `id` field, but a specific record has been specified"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ bio: 'Again', id: 20 }]"

[[test.results]]
value = "[{ bio: 'Founder', id: 1 }, { bio: 'Tester', id: 3 }, { bio: 'Again', id: 20 }, { bio: 'Nobody', id: 99 }]"

[[test.results]]
value = "[{ id: 3 }, { id: 4 }]"

[[test.results]]
value = "[{ hit: NONE, id: 1 }, { hit: NONE, id: 2 }, { hit: true, id: 3 }, { hit: true, id: 4 }, { hit: NONE, id: 5 }, { hit: NONE, id: 6 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 3 }, { id: 4 }]"

[[test.results]]
value = "[{ hit: 'in', id: 1 }, { hit: NONE, id: 2 }, { hit: 'in', id: 3 }, { hit: 'in', id: 4 }, { hit: NONE, id: 5 }, { hit: NONE, id: 6 }]"

[[test.results]]
value = "[{ id: 3 }, { id: 4 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }, { id: 5 }, { id: 6 }]"

*/

UPDATE person SET age = 43 WHERE id = 1 RETURNING id, name, age;

UPDATE person SET age = 0 WHERE id IN (2, 3) RETURNING id;

UPDATE person SET age = 1 WHERE id = 99 RETURNING id;

UPDATE person SET age = 5 WHERE id > 9 RETURNING id, age;

UPDATE person SET age = id * 100 WHERE id = 10 RETURNING id, age;

UPDATE person SET name = 'x' WHERE id = 1 AND age = 43 RETURNING id;

UPDATE person SET name = 'y' WHERE id = 1 AND age = 0 RETURNING id;

UPDATE person SET age = age + 1 WHERE person.id = 3 RETURNING person.id, age;

UPDATE person SET age = 9 WHERE id = '1' RETURNING id;

UPDATE person SET age = 9 WHERE id = 'person:1' RETURNING id;

SELECT id, name, age FROM person ORDER BY id;

UPDATE tag SET label = 'Rust!' WHERE id = 'rust' RETURNING id, label;

UPDATE tag SET label = 'Spelled' WHERE id = 'person:1' RETURNING id, label;

UPDATE tag SET label = 'One' WHERE id = '1' RETURNING *;

UPDATE person SET age = 7 RETURNING id, name;

SELECT id, name, age FROM person ORDER BY id;

DELETE FROM person WHERE id = 2 RETURNING id, name;

DELETE FROM person WHERE id = 99 RETURNING id;

SELECT id FROM person ORDER BY id;

DELETE FROM person WHERE id IN (1, 3);

SELECT id FROM person ORDER BY id;

INSERT INTO person (id, name, age) VALUES (2, 'Back', 1) RETURNING id;

DELETE FROM person WHERE id = 2 RETURNING *;

DELETE FROM person WHERE id > 5;

SELECT id FROM person ORDER BY id;

DELETE FROM tag WHERE id = 'db' RETURNING id;

SELECT id, label FROM tag ORDER BY id;

INSERT INTO person (id, name, age) VALUES (20, 'New', 1) RETURNING id, name;

SELECT id, name FROM person WHERE id = 20;

INSERT INTO person (id, name, age) VALUES (20, 'Again', 2) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, age = age + 1 RETURNING id, name, age;

INSERT INTO person (id, name, age) VALUES (20, 'Never', 3) ON CONFLICT (id) DO NOTHING RETURNING id;

INSERT INTO person (id, name, age) VALUES (21, 'Other', 3) ON CONFLICT (id) DO NOTHING RETURNING id;

SELECT id, name, age FROM person ORDER BY id;

INSERT INTO tag (id, label) VALUES ('new', 'New tag') RETURNING id, label;

SELECT id, label FROM tag WHERE id = 'new';

UPDATE person SET id = 5 WHERE id = 20;

SELECT id FROM person WHERE id = 5;

INSERT INTO profile (id, bio) SELECT id, name AS bio FROM person WHERE id = 20;

SELECT id, bio FROM profile ORDER BY id;

UPDATE node SET hit = true WHERE id = up RETURNING id;

SELECT id, hit FROM node ORDER BY id;

UPDATE node SET hit = 'in' WHERE id IN (up, 1) RETURNING id;

SELECT id, hit FROM node ORDER BY id;

DELETE FROM node WHERE id = up RETURNING id;

SELECT id FROM node ORDER BY id;
