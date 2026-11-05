/**
[test]
reason = "Statements that differ only in the record id written as their source each answer their own record, whatever the cache of compiled statements keeps"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 30, id: pk:1, name: 'p1' }]"

[[test.results]]
value = "[{ age: 31, id: pk:2, name: 'p2' }]"

[[test.results]]
value = "[{ age: 30, id: pk:3, name: 'p3' }]"

[[test.results]]
value = "[{ age: 9, id: pk:x, name: 'px' }]"

[[test.results]]
value = "[{ id: ls:1, name: 'l1' }]"

[[test.results]]
value = "[{ id: ls:[1, 'a'], name: 'la' }]"

[[test.results]]
value = "[{ id: ls:{ k: 1 }, name: 'lo' }]"

[[test.results]]
value = "[{ age: 30, id: pk:1, name: 'p1' }]"

[[test.results]]
value = "[{ age: 31, id: pk:2, name: 'p2' }]"

[[test.results]]
value = "[{ age: 9, id: pk:x, name: 'px' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: ls:1, name: 'l1' }]"

[[test.results]]
value = "[{ id: ls:[1, 'a'], name: 'la' }]"

[[test.results]]
value = "[{ id: ls:{ k: 1 }, name: 'lo' }]"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
value = "{ name: 'p3' }"

[[test.results]]
value = "{ name: 'p1' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['p1']"

[[test.results]]
value = "['p2']"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "['p1', 'l1']"

[[test.results]]
value = "['l1', 'p3']"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ name: 'p2' }]"

[[test.results]]
value = "[{ name: 'px' }]"

[[test.results]]
value = "[{ name: 'p1', y: 3 }]"

[[test.results]]
value = "[{ name: 'p2', y: 8 }]"

[[test.results]]
value = "[{ name: 'p3', tag: 'a' }]"

[[test.results]]
value = "[{ name: 'p3', tag: 'b' }]"

[[test.results]]
value = "['p1', 'p2']"

[[test.results]]
value = "['p2', 'p3', 'px']"

[[test.results]]
value = "['p1', 'p3']"

[[test.results]]
value = "['p2']"

[[test.results]]
value = "['p2']"

[[test.results]]
value = "['p3']"

[[test.results]]
value = "[{ name: 'p1', other: ['p2'] }]"

[[test.results]]
value = "[{ name: 'p1', other: ['p3'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'l1l1'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ id: ls:1, name: 'l1' }"

*/

DEFINE TABLE pk SCHEMAFULL;
DEFINE FIELD name ON pk TYPE string;
DEFINE FIELD age ON pk TYPE int;
DEFINE INDEX pk_age ON pk FIELDS age;
CREATE pk:1 SET name = 'p1', age = 30;
CREATE pk:2 SET name = 'p2', age = 31;
CREATE pk:3 SET name = 'p3', age = 30;
CREATE pk:x SET name = 'px', age = 9;
CREATE ls:1 SET name = 'l1';
CREATE ls:[1, 'a'] SET name = 'la';
CREATE ls:{ k: 1 } SET name = 'lo';

SELECT * FROM pk:1;
SELECT * FROM pk:2;
SELECT * FROM pk:x;
SELECT * FROM pk:9;
SELECT * FROM ls:1;
SELECT * FROM ls:[1, 'a'];
SELECT * FROM ls:{ k: 1 };
SELECT * FROM nosuch:1;
SELECT name FROM ONLY pk:3;
SELECT name FROM ONLY pk:1;
SELECT name FROM ONLY pk:9;
SELECT VALUE name FROM pk:1 LIMIT 1;
SELECT VALUE name FROM pk:2 LIMIT 1;
SELECT VALUE name FROM pk:2 LIMIT 0;
SELECT VALUE name FROM pk:3 START 1;
SELECT VALUE name FROM [pk:1, ls:1];
SELECT VALUE name FROM [ls:1, pk:3, pk:9];
SELECT VALUE name FROM [];
SELECT name FROM type::record('pk', 2);
SELECT name FROM type::record('pk', 'x');
SELECT name, (|$x| $x + 1)(2) AS y FROM pk:1;
SELECT name, (|$x| $x + 1)(7) AS y FROM pk:2;
SELECT name, 'a' AS tag FROM pk:3;
SELECT name, 'b' AS tag FROM pk:3;
SELECT VALUE name FROM pk:1..=2;
SELECT VALUE name FROM pk:2..;
SELECT VALUE name FROM pk WHERE age = 30 ORDER BY name;
SELECT VALUE name FROM pk WHERE age = 31 ORDER BY name;
SELECT VALUE name FROM pk WHERE id = pk:2;
SELECT VALUE name FROM pk WHERE id = pk:3;
SELECT name, (SELECT VALUE name FROM pk:2) AS other FROM pk:1;
SELECT name, (SELECT VALUE name FROM pk:3) AS other FROM pk:1;
DEFINE FIELD twice ON ls COMPUTED string::concat(name, name);
SELECT VALUE twice FROM ONLY ls:1;
REMOVE FIELD twice ON ls;
SELECT * FROM ONLY ls:1;
