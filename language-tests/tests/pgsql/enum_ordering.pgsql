/**
[env]
namespace = true
database = true
imports = ["pgsql/_enum_schema.surql"]

[test]
reason = "PostgreSQL orders and compares an enum column by declaration order, not by the spelling of its labels, and MIN and MAX follow it"

[[test.results]]
value = "[{ name: 'b' }, { name: 'c' }, { name: 'a' }]"

[[test.results]]
value = "[{ name: 'a' }, { name: 'c' }, { name: 'b' }]"

[[test.results]]
value = "[{ name: 'a' }]"

[[test.results]]
value = "[{ name: 'b' }, { name: 'c' }]"

[[test.results]]
value = "[{ name: 'a' }, { name: 'c' }]"

[[test.results]]
value = "[{ name: 'b' }, { name: 'c' }]"

[[test.results]]
value = "[{ name: 'b' }, { name: 'c' }]"

[[test.results]]
value = "[{ name: 'a' }]"

[[test.results]]
value = "[{ hi: 'happy', lo: 'sad' }]"

[[test.results]]
value = "[{ k: 'x', lo: 'sad' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 4, m: 'ecstatic', name: 'd' }, { id: 5, m: 'meh', name: 'e' }]"

[[test.results]]
value = "[{ name: 'b' }, { name: 'e' }, { name: 'c' }, { name: 'a' }, { name: 'd' }]"

[[test.results]]
value = "[{ name: 'a' }, { name: 'd' }]"

[[test.results]]
value = "[{ name: 'e' }, { name: 'b' }]"

[[test.results]]
value = "[{ hi: 'ecstatic', lo: 'sad' }]"

[[test.results]]
error = "'nope' is not a value of the type 'mood'; its values are 'sad', 'meh', 'ok', 'happy', 'ecstatic'"

[[test.results]]
value = "[{ id: 6, m: NULL, name: 'f' }]"

[[test.results]]
value = "[{ name: 'b' }, { name: 'e' }, { name: 'c' }, { name: 'a' }, { name: 'd' }, { name: 'f' }]"

[[test.results]]
value = "[{ name: 'f' }, { name: 'd' }, { name: 'a' }, { name: 'c' }, { name: 'e' }, { name: 'b' }]"

[[test.results]]
value = "[{ name: 'f' }, { name: 'b' }, { name: 'e' }, { name: 'c' }, { name: 'a' }, { name: 'd' }]"

[[test.results]]
value = "[{ name: 'b' }, { name: 'e' }, { name: 'c' }]"

[[test.results]]
value = "[{ held: 5, hi: 'ecstatic', lo: 'sad' }]"

[[test.results]]
value = "[{ name: 'b' }, { name: 'e' }, { name: 'c' }, { name: 'a' }, { name: 'd' }, { name: 'f' }]"

[[test.results]]
value = "[{ name: 'a' }, { name: 'd' }]"

[[test.results]]
value = "[{ name: 'b' }, { name: 'e' }, { name: 'c' }, { name: 'a' }, { name: 'd' }, { name: 'f' }]"
*/

SELECT name FROM person ORDER BY m;

SELECT name FROM person ORDER BY m DESC;

SELECT name FROM person WHERE m > 'ok';

SELECT name FROM person WHERE m < 'happy' ORDER BY name;

SELECT name FROM person WHERE m >= 'ok' ORDER BY name;

SELECT name FROM person WHERE m <= 'ok' ORDER BY name;

SELECT name FROM person WHERE m BETWEEN 'sad' AND 'ok' ORDER BY name;

SELECT name FROM person WHERE m NOT BETWEEN 'sad' AND 'ok' ORDER BY name;

SELECT min(m) AS lo, max(m) AS hi FROM person;

SELECT 'x' AS k, min(m) AS lo FROM person GROUP BY 1;

ALTER TYPE mood ADD VALUE 'ecstatic' AFTER 'happy';

ALTER TYPE mood ADD VALUE 'meh' BEFORE 'ok';

INSERT INTO person (id, name, m) VALUES (4, 'd', 'ecstatic'), (5, 'e', 'meh');

SELECT name FROM person ORDER BY m;

SELECT name FROM person WHERE m > 'ok' ORDER BY m;

SELECT name FROM person WHERE m < 'ok' ORDER BY m DESC;

SELECT min(m) AS lo, max(m) AS hi FROM person;

SELECT name FROM person WHERE m > 'nope';

INSERT INTO person (id, name) VALUES (6, 'f');

SELECT name FROM person ORDER BY m;

SELECT name FROM person ORDER BY m DESC;

SELECT name FROM person ORDER BY m NULLS FIRST;

SELECT name FROM person WHERE m < 'happy' ORDER BY m;

SELECT count(m) AS held, min(m) AS lo, max(m) AS hi FROM person;

SELECT name FROM (SELECT name, m FROM person) AS p ORDER BY m, name;

WITH p AS (SELECT name, m FROM person) SELECT name FROM p WHERE m > 'ok' ORDER BY m;

SELECT a.name FROM person a JOIN person b ON a.id = b.id ORDER BY a.m, a.name;
