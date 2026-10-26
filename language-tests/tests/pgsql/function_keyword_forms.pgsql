/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL functions with keyword syntax: extract FROM, position IN, substring FROM and FOR, overlay PLACING, and left and right"

[[test.results]]
value = "[{ dow: 5, m: 3, y: 2024 }]"

[[test.results]]
value = "[{ a: 4, b: 0, c: 1, d: 0 }]"

[[test.results]]
value = "[{ a: 'Wor', b: 'ello', c: 'He', d: 'He' }]"

[[test.results]]
value = "[{ a: 'Thomas', b: 'Thomxas', c: NULL }]"

[[test.results]]
value = "[{ a: 'He', b: 'lo', c: 'Hel', d: 'llo', e: 'Hello', f: NULL }]"
*/

SELECT extract(year FROM CAST('2024-03-15T10:20:30Z' AS timestamp)) AS y, extract(month FROM CAST('2024-03-15T10:20:30Z' AS timestamp)) AS m, extract(dow FROM CAST('2024-03-15T10:20:30Z' AS timestamp)) AS dow;

SELECT position('lo' IN 'Hello') AS a, position('LO' IN 'Hello') AS b, position('' IN 'Hello') AS c, position('x' IN 'Hello') AS d;

SELECT substring('Hello World' FROM 7 FOR 3) AS a, substring('Hello' FROM 2) AS b, substring('Hello' FOR 2) AS c, substring('Hello' FROM 0 FOR 3) AS d;

SELECT overlay('Txxxxas' PLACING 'hom' FROM 2 FOR 4) AS a, overlay('Txxxxas' PLACING 'hom' FROM 2) AS b, overlay(NULL PLACING 'hom' FROM 2) AS c;

SELECT left('Hello', 2) AS a, right('Hello', 2) AS b, left('Hello', -2) AS c, right('Hello', -2) AS d, left('Hello', 9) AS e, left(NULL, 1) AS f;
