/**
[env]
namespace = true
database = true
imports = ["pgsql/_link_fields.surql"]

[test]
reason = "A plain key written to a column declared as a link to one table stores the link to that table: an integer or string literal, an expression, a multi-row VALUES, a column order unlike the table's, UPDATE SET, ON CONFLICT DO UPDATE and INSERT ... SELECT. SQL NULL written to a link column whose type admits NONE (`option<record<t>>`) is stored as NONE and reads back as NULL; a link column whose type does not admit NONE refuses it. A value that is not a key (a float, a boolean, the empty string), a link to another table, and a key written to a column that links to any table or to several types, reaches the engine's own type check and is refused or kept as the type says."

[[test.results]]
value = "[{ dept: dept:2, id: 5, name: 'Kim' }]"

[[test.results]]
value = "[{ dept: dept:ops, id: 6 }]"

[[test.results]]
value = "[{ dept: dept:2, id: 7 }]"

[[test.results]]
value = "[{ dept: dept:1, id: 8 }]"

[[test.results]]
value = "[{ dept: dept:1, id: 9 }, { dept: dept:2, id: 10 }]"

[[test.results]]
value = "[{ dept: dept:2, id: 1 }]"

[[test.results]]
value = "[{ dept: dept:ops, id: 2, note: 'moved' }]"

[[test.results]]
value = "[{ dept: dept:1, id: 5 }]"

[[test.results]]
value = "[{ dept: dept:2, id: 5 }]"

[[test.results]]
value = "[{ dept: dept:1, id: 21, name: 'Engineering' }]"

[[test.results]]
error = "Couldn't coerce value for field `dept` of `emp:12`: Expected `record<dept>` but found `1.5dec`"

[[test.results]]
error = "Couldn't coerce value for field `dept` of `emp:12`: Expected `record<dept>` but found `NULL`"

[[test.results]]
error = "Couldn't coerce value for field `dept` of `emp:12`: Expected `record<dept>` but found `''`"

[[test.results]]
error = "Couldn't coerce value for field `dept` of `emp:12`: Expected `record<dept>` but found `true`"

[[test.results]]
error = "Couldn't coerce value for field `dept` of `emp:3`: Expected `record<dept>` but found `emp:1`"

[[test.results]]
value = "[{ id: 1, owner: dept:1 }]"

[[test.results]]
value = "[{ id: 2, owner: NONE }]"

[[test.results]]
value = "[{ id: 1, owner: NONE }]"

[[test.results]]
value = "[{ id: 3, owner: NONE }]"

[[test.results]]
error = "Couldn't coerce value for field `anyref` of `project:4`: Expected `none | record` but found `1`"

[[test.results]]
value = "[{ either: 7, id: 5 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 5 }, { id: 7 }, { id: 10 }]"

[[test.results]]
value = "[{ dept: dept:ops, id: 2, name: 'Sam' }, { dept: dept:1, id: 3, name: 'Ann' }, { dept: dept:ops, id: 4, name: 'Bob' }, { dept: dept:ops, id: 6, name: 'Lou' }, { dept: dept:1, id: 8, name: 'Ned' }, { dept: dept:1, id: 9, name: 'Oli' }, { dept: dept:1, id: 21, name: 'Engineering' }]"
*/

INSERT INTO emp (id, name, dept) VALUES (5, 'Kim', 2) RETURNING id, name, dept;

INSERT INTO emp (id, name, dept) VALUES (6, 'Lou', 'ops') RETURNING id, dept;

INSERT INTO emp (id, name, dept) VALUES (7, 'Max', 1 + 1) RETURNING id, dept;

INSERT INTO emp (dept, name, id) VALUES (1, 'Ned', 8) RETURNING id, dept;

INSERT INTO emp (id, name, dept) VALUES (9, 'Oli', 1), (10, 'Pat', 2) RETURNING id, dept;

UPDATE emp SET dept = 2 WHERE id = 1 RETURNING id, dept;

UPDATE emp SET dept = 'ops', note = 'moved' WHERE name = 'Sam' RETURNING id, dept, note;

INSERT INTO emp (id, name, dept) VALUES (5, 'Kim', 1) ON CONFLICT (id) DO UPDATE SET dept = EXCLUDED.dept RETURNING id, dept;

INSERT INTO emp (id, name, dept) VALUES (5, 'Kim', 1) ON CONFLICT (id) DO UPDATE SET dept = 2 RETURNING id, dept;

INSERT INTO emp (id, name, dept) SELECT id + 20 AS id, dname AS name, id AS dept FROM dept WHERE id = 1 RETURNING id, name, dept;

INSERT INTO emp (id, name, dept) VALUES (12, 'Bad', 1.5);

INSERT INTO emp (id, name, dept) VALUES (12, 'Bad', NULL);

INSERT INTO emp (id, name, dept) VALUES (12, 'Bad', '');

INSERT INTO emp (id, name, dept) VALUES (12, 'Bad', true);

UPDATE emp SET dept = mentor WHERE id = 3;

INSERT INTO project (id, title, owner) VALUES (1, 'Alpha', 1) RETURNING id, owner;

INSERT INTO project (id, title, owner) VALUES (2, 'Beta', NULL) RETURNING id, owner;

UPDATE project SET owner = NULL WHERE id = 1 RETURNING id, owner;

INSERT INTO project (id, title) VALUES (3, 'Gamma') RETURNING id, owner;

INSERT INTO project (id, title, anyref) VALUES (4, 'Delta', 1);

INSERT INTO project (id, title, either) VALUES (5, 'Eps', 7) RETURNING id, either;

DELETE FROM emp WHERE dept = 2 RETURNING id;

SELECT id, name, dept FROM emp ORDER BY id;
