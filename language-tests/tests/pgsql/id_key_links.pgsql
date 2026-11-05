/**
[env]
namespace = true
database = true
imports = ["pgsql/_id_key.surql"]

[test]
reason = "A column or an expression compared with the id of a PostgreSQL table, in a join, a subquery, a predicate or a projection, names the row whether it holds the key the row has (a foreign key written through SQL) or a record link to the row (a foreign key written as a record id), and a value that names no row, NULL included, matches nothing. The id of another table is compared by key."

[[test.results]]
value = "[{ name: 'Tobie', title: 'Link one' }, { name: 'Jaime', title: 'Link three' }, { name: 'Tobie', title: 'Link two' }]"

[[test.results]]
value = "[{ name: 'Tobie', title: 'Link one' }, { name: 'Jaime', title: 'Link three' }, { name: 'Tobie', title: 'Link two' }]"

[[test.results]]
value = "[{ name: NONE, title: 'Dangling link' }, { name: 'Tobie', title: 'Link one' }, { name: 'Jaime', title: 'Link three' }, { name: 'Tobie', title: 'Link two' }, { name: NONE, title: 'Unassigned' }]"

[[test.results]]
value = "[{ name: 'Jaime', title: 'Link three' }, { name: 'Tobie', title: 'Link one' }, { name: 'Tobie', title: 'Link two' }]"

[[test.results]]
value = "[{ name: 'Jaime', title: 'Link three' }, { name: 'Tobie', title: 'Link one' }, { name: 'Tobie', title: 'Link two' }]"

[[test.results]]
value = "[{ name: 'Jaime', title: 'Link three' }, { name: 'Ten', title: NONE }, { name: 'Tobie', title: 'Link one' }, { name: 'Tobie', title: 'Link two' }, { name: 'Zed', title: NONE }]"

[[test.results]]
value = "[{ name: 'Jaime', title: 'Again' }, { name: 'Tobie', title: 'Hello' }, { name: 'Tobie', title: 'World' }]"

[[test.results]]
value = "[{ id: 1, name: 'Tobie' }, { id: 2, name: 'Jaime' }, { id: 3, name: 'Zed' }]"

[[test.results]]
value = "[{ id: 2, name: 'Jaime' }, { id: 1, name: 'Tobie' }, { id: 3, name: 'Zed' }]"

[[test.results]]
value = "[{ id: 1, name: 'Tobie' }, { id: 2, name: 'Jaime' }, { id: 3, name: 'Zed' }, { id: 4, name: NONE }, { id: 5, name: NONE }, { id: 6, name: NONE }]"

[[test.results]]
value = "[{ id: 6, label: 'Rust' }]"

[[test.results]]
value = "[{ id: 1, tickets: 2 }, { id: 2, tickets: 1 }, { id: 3, tickets: 0 }, { id: 10, tickets: 0 }]"

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }, { name: 'Zed' }]"

[[test.results]]
value = "[{ id: 1, who: 'Tobie' }, { id: 2, who: 'Jaime' }, { id: 3, who: 'Zed' }, { id: 4, who: NONE }, { id: 5, who: NONE }, { id: 6, who: NONE }]"

[[test.results]]
value = "[{ title: 'Dangling link', who: NONE }, { title: 'Link one', who: 'Tobie' }, { title: 'Link three', who: 'Jaime' }, { title: 'Link two', who: 'Tobie' }, { title: 'Unassigned', who: NONE }]"

[[test.results]]
value = "[{ id: 3 }, { id: 4 }]"

[[test.results]]
value = "[{ id: 3 }, { id: 4 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 3 }, { id: 4 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 3 }, { id: 4 }]"

[[test.results]]
value = "[{ id: 4 }]"

[[test.results]]
value = "[{ id: 1, own_parent: false }, { id: 2, own_parent: false }, { id: 3, own_parent: true }, { id: 4, own_parent: true }, { id: 5, own_parent: NULL }, { id: 6, own_parent: NULL }]"

[[test.results]]
value = "[{ id: 1, kind: 'other' }, { id: 2, kind: 'other' }, { id: 3, kind: 'self' }, { id: 4, kind: 'self' }, { id: 5, kind: 'other' }, { id: 6, kind: 'other' }]"

[[test.results]]
value = "[{ id: 1, other: 2 }, { id: 2, other: 3 }, { id: 3, other: 3 }, { id: 4, other: 4 }]"

[[test.results]]
value = "[{ id: 2, other: 1 }, { id: 3, other: 2 }, { id: 3, other: 3 }, { id: 4, other: 4 }]"

[[test.results]]
value = "[{ id: 2, other: 1 }, { id: 3, other: 2 }]"

[[test.results]]
value = "[{ id: 3 }, { id: node:2 }, { id: node:3 }]"

*/

SELECT t.title, p.name FROM ticket t JOIN person p ON t.author = p.id ORDER BY t.title;

SELECT t.title, p.name FROM ticket t JOIN person p ON p.id = t.author ORDER BY t.title;

SELECT t.title, p.name FROM ticket t LEFT JOIN person p ON t.author = p.id ORDER BY t.title;

SELECT p.name, t.title FROM person p JOIN ticket t ON t.author = p.id ORDER BY p.name, t.title;

SELECT p.name, t.title FROM person p JOIN ticket t ON p.id = t.author ORDER BY p.name, t.title;

SELECT p.name, t.title FROM person p LEFT JOIN ticket t ON t.author = p.id ORDER BY p.name, t.title;

SELECT p.name, o.title FROM person p JOIN post o ON o.author = p.id ORDER BY p.name, o.title;

SELECT n.id, p.name FROM note n JOIN person p ON n.ref = p.id ORDER BY n.id;

SELECT p.name, n.id FROM person p JOIN note n ON n.ref = p.id ORDER BY p.name, n.id;

SELECT n.id, p.name FROM note n LEFT JOIN person p ON n.ref = p.id ORDER BY n.id;

SELECT n.id, g.label FROM note n JOIN tag g ON n.ref = g.id ORDER BY n.id;

SELECT p.id, count(t.id) AS tickets FROM person p LEFT JOIN ticket t ON t.author = p.id GROUP BY p.id ORDER BY p.id;

SELECT p.name FROM person p WHERE EXISTS (SELECT 1 FROM ticket t WHERE t.author = p.id) ORDER BY p.name;

SELECT p.name FROM person p WHERE EXISTS (SELECT 1 FROM post o WHERE o.author = p.id) ORDER BY p.name;

SELECT p.name FROM person p WHERE EXISTS (SELECT 1 FROM note n WHERE n.ref = p.id) ORDER BY p.name;

SELECT n.id, (SELECT p.name FROM person p WHERE p.id = n.ref) AS who FROM note n ORDER BY n.id;

SELECT t.title, (SELECT p.name FROM person p WHERE p.id = t.author) AS who FROM ticket t ORDER BY t.title;

SELECT id FROM node WHERE id = up ORDER BY id;

SELECT id FROM node WHERE up = id ORDER BY id;

SELECT id FROM node WHERE id <> up ORDER BY id;

SELECT id FROM node WHERE NOT (id = up) ORDER BY id;

SELECT id FROM node WHERE id = up OR id = 1 ORDER BY id;

SELECT id FROM node WHERE id IN (up, 1) ORDER BY id;

SELECT id FROM node WHERE id = up AND id > 3 ORDER BY id;

SELECT id, id = up AS own_parent FROM node ORDER BY id;

SELECT id, CASE WHEN id = up THEN 'self' ELSE 'other' END AS kind FROM node ORDER BY id;

SELECT a.id, b.id AS other FROM node a JOIN node b ON a.up = b.id ORDER BY a.id;

SELECT a.id, b.id AS other FROM node a JOIN node b ON b.up = a.id ORDER BY a.id, b.id;

SELECT a.id, b.id AS other FROM node a JOIN node b ON a.id = b.up AND a.id <> b.id ORDER BY a.id;

SELECT d.id FROM (SELECT up AS id FROM node) d JOIN person p ON p.id = d.id ORDER BY d.id;
