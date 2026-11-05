/**
[env]
namespace = true
database = true
imports = ["pgsql/_id_key.surql"]

[test]
reason = "Joins on the id column of PostgreSQL tables: a table joined to itself and the 1:1 extension of a table by its id match on the key, and a plain integer column that holds the id of another table (a foreign key) matches that id in either direction, inner and left, in subqueries and with grouping. The unmatched side of a left join has no id, so a comparison of it is NULL, as under NOT."

[[test.results]]
value = "[{ id: 1, other: 1 }, { id: 2, other: 2 }, { id: 3, other: 3 }, { id: 10, other: 10 }]"

[[test.results]]
value = "[{ name: 'Tobie', other: 'Tobie' }, { name: 'Jaime', other: 'Jaime' }]"

[[test.results]]
value = "[{ id: 1, other: 1 }, { id: 2, other: 2 }, { id: 3, other: NONE }, { id: 10, other: NONE }]"

[[test.results]]
value = "[{ bio: 'Founder', id: 1, name: 'Tobie' }, { bio: 'Tester', id: 3, name: 'Zed' }]"

[[test.results]]
value = "[{ bio: 'Founder', id: 1, name: 'Tobie' }, { bio: 'Tester', id: 3, name: 'Zed' }]"

[[test.results]]
value = "[{ bio: 'Founder', id: 1 }, { bio: NONE, id: 2 }, { bio: 'Tester', id: 3 }, { bio: NONE, id: 10 }]"

[[test.results]]
value = "[{ id: 1, name: 'Tobie' }, { id: 3, name: 'Zed' }, { id: 99, name: NONE }]"

[[test.results]]
value = "[{ fid: 1, id: 1 }, { fid: NONE, id: 2 }, { fid: 3, id: 3 }, { fid: NONE, id: 10 }]"

[[test.results]]
value = "[{ bio: 'Founder', id: 1 }, { bio: 'Tester', id: 3 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }, { id: 3 }, { id: 10 }]"

[[test.results]]
value = "[{ name: 'Jaime', title: 'Again' }, { name: 'Tobie', title: 'Hello' }, { name: 'Tobie', title: 'World' }]"

[[test.results]]
value = "[{ name: 'Jaime', title: 'Again' }, { name: 'Tobie', title: 'Hello' }, { name: 'Tobie', title: 'World' }]"

[[test.results]]
value = "[{ name: 'Jaime', title: 'Again' }, { name: 'Tobie', title: 'Hello' }, { name: 'Tobie', title: 'World' }]"

[[test.results]]
value = "[{ name: 'Jaime', title: 'Again' }, { name: 'Tobie', title: 'Hello' }, { name: 'Tobie', title: 'World' }]"

[[test.results]]
value = "[{ name: 'Jaime', title: 'Again' }, { name: NONE, title: 'Anonymous' }, { name: 'Tobie', title: 'Hello' }, { name: NONE, title: 'Orphan' }, { name: 'Tobie', title: 'World' }]"

[[test.results]]
value = "[{ name: 'Jaime', title: 'Again' }, { name: 'Ten', title: NONE }, { name: 'Tobie', title: 'Hello' }, { name: 'Tobie', title: 'World' }, { name: 'Zed', title: NONE }]"

[[test.results]]
value = "[{ id: 1, post_id: 1 }, { id: 1, post_id: 2 }, { id: 2, post_id: 3 }, { id: 3, post_id: NONE }, { id: 10, post_id: NONE }]"

[[test.results]]
value = "[{ id: 1, posts: 2 }, { id: 2, posts: 1 }, { id: 3, posts: 0 }, { id: 10, posts: 0 }]"

[[test.results]]
value = "[{ id: 1, name: 'Tobie', posts: 2 }, { id: 2, name: 'Jaime', posts: 1 }]"

[[test.results]]
value = "[{ id: 1, title: 'Hello' }, { id: 1, title: 'World' }]"

[[test.results]]
value = "[{ title: 'Again' }]"

[[test.results]]
value = "[{ title: 'Hello' }, { title: 'World' }]"

[[test.results]]
value = "[{ bio: 'Founder', name: 'Tobie', title: 'Hello' }, { bio: 'Founder', name: 'Tobie', title: 'World' }]"

[[test.results]]
value = "[{ bio: NONE, name: 'Jaime', title: 'Again' }, { bio: 'Founder', name: 'Tobie', title: 'Hello' }, { bio: 'Founder', name: 'Tobie', title: 'World' }]"

[[test.results]]
value = "[{ title: 'Again' }, { title: 'Hello' }, { title: 'World' }]"

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"

[[test.results]]
value = "[{ name: 'Ten' }, { name: 'Zed' }]"

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"

[[test.results]]
value = "[{ name: 'Ten' }, { name: 'Zed' }]"

[[test.results]]
value = "[{ bio: 'Founder', id: 1 }, { bio: NONE, id: 2 }, { bio: 'Tester', id: 3 }, { bio: NONE, id: 10 }]"

[[test.results]]
value = "[{ first_post: 'Hello', id: 1 }, { first_post: 'Again', id: 2 }, { first_post: NONE, id: 3 }, { first_post: NONE, id: 10 }]"

[[test.results]]
value = "[{ id: 1, name: 'Tobie' }, { id: 3, name: 'Zed' }]"

[[test.results]]
value = "[{ id: 3 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 3 }]"

[[test.results]]
value = "[{ first_profile: true, id: 1 }, { first_profile: NULL, id: 2 }, { first_profile: false, id: 3 }, { first_profile: NULL, id: 10 }]"

*/

SELECT a.id, b.id AS other FROM person a JOIN person b ON a.id = b.id ORDER BY a.id;

SELECT a.name, b.name AS other FROM person a JOIN person b ON a.id = b.id AND a.age > 20 ORDER BY a.id;

SELECT a.id, b.id AS other FROM person a LEFT JOIN person b ON a.id = b.id AND b.age > 20 ORDER BY a.id;

SELECT p.id, p.name, f.bio FROM person p JOIN profile f ON f.id = p.id ORDER BY p.id;

SELECT p.id, p.name, f.bio FROM person p JOIN profile f ON p.id = f.id ORDER BY p.id;

SELECT p.id, f.bio FROM person p LEFT JOIN profile f ON f.id = p.id ORDER BY p.id;

SELECT f.id, p.name FROM profile f LEFT JOIN person p ON p.id = f.id ORDER BY f.id;

SELECT p.id, f.id AS fid FROM person p LEFT JOIN profile f ON f.id = p.id ORDER BY p.id;

SELECT p.id, f.bio FROM person p JOIN profile f USING (id) ORDER BY p.id;

SELECT a.id FROM person a JOIN person b USING (id) ORDER BY a.id;

SELECT o.title, p.name FROM post o JOIN person p ON o.author = p.id ORDER BY o.title;

SELECT o.title, p.name FROM post o JOIN person p ON p.id = o.author ORDER BY o.title;

SELECT p.name, o.title FROM person p JOIN post o ON o.author = p.id ORDER BY p.name, o.title;

SELECT p.name, o.title FROM person p JOIN post o ON p.id = o.author ORDER BY p.name, o.title;

SELECT o.title, p.name FROM post o LEFT JOIN person p ON o.author = p.id ORDER BY o.title;

SELECT p.name, o.title FROM person p LEFT JOIN post o ON o.author = p.id ORDER BY p.name, o.title;

SELECT p.id, o.id AS post_id FROM person p LEFT JOIN post o ON o.author = p.id ORDER BY p.id, o.id;

SELECT p.id, count(o.id) AS posts FROM person p LEFT JOIN post o ON o.author = p.id GROUP BY p.id ORDER BY p.id;

SELECT p.id, p.name, count(*) AS posts FROM person p JOIN post o ON o.author = p.id GROUP BY p.id, p.name ORDER BY p.id;

SELECT o.title, p.id FROM post o JOIN person p ON o.author = p.id WHERE p.id = 1 ORDER BY o.title;

SELECT o.title FROM post o JOIN person p ON o.author = p.id AND p.id = 2;

SELECT o.title FROM post o JOIN person p ON o.author = p.id WHERE p.age > 41 ORDER BY o.title;

SELECT o.title, p.name, f.bio FROM post o JOIN person p ON o.author = p.id JOIN profile f ON f.id = p.id ORDER BY o.title;

SELECT o.title, p.name, f.bio FROM post o JOIN person p ON o.author = p.id LEFT JOIN profile f ON f.id = p.id ORDER BY o.title;

SELECT o.title FROM post o WHERE o.author IN (SELECT id FROM person WHERE age > 40) ORDER BY o.title;

SELECT p.name FROM person p WHERE p.id IN (SELECT author FROM post) ORDER BY p.name;

SELECT p.name FROM person p WHERE p.id NOT IN (SELECT author FROM post WHERE author IS NOT NULL) ORDER BY p.name;

SELECT p.name FROM person p WHERE EXISTS (SELECT 1 FROM post o WHERE o.author = p.id) ORDER BY p.name;

SELECT p.name FROM person p WHERE NOT EXISTS (SELECT 1 FROM post o WHERE o.author = p.id) ORDER BY p.name;

SELECT p.id, (SELECT f.bio FROM profile f WHERE f.id = p.id) AS bio FROM person p ORDER BY p.id;

SELECT p.id, (SELECT o.title FROM post o WHERE o.author = p.id ORDER BY o.title LIMIT 1) AS first_post FROM person p ORDER BY p.id;

SELECT p.id, p.name FROM person p, profile f WHERE f.id = p.id ORDER BY p.id;





SELECT p.id FROM person p LEFT JOIN profile f ON f.id = p.id WHERE NOT (f.id = 1) ORDER BY p.id;

SELECT p.id FROM person p LEFT JOIN profile f ON f.id = p.id WHERE f.id = 1 OR f.id = 3 ORDER BY p.id;

SELECT p.id, f.id = 1 AS first_profile FROM person p LEFT JOIN profile f ON f.id = p.id ORDER BY p.id;
