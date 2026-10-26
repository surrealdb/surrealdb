/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL substring and substr count positions from 1, let a start before 1 use up the count, and answer NULL for a NULL argument"

[[test.results]]
value = "[{ a: 'Hello', b: 'World', c: 'Wor' }]"

[[test.results]]
value = "[{ a: 'He', b: 'H', c: '', d: '', e: NULL }]"
*/

SELECT substring('Hello World', 1, 5) AS a, substring('Hello World', 7) AS b, substr('Hello World', 7, 3) AS c;

SELECT substring('Hello', 0, 3) AS a, substring('Hello', -2, 4) AS b, substring('Hello', 9) AS c, substring('Hello', 2, 0) AS d, substring(NULL, 1) AS e;
