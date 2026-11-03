/**
[env]
namespace = true
database = true
imports = ["pgsql/_enum_schema.surql"]

[test]
reason = "Each query of a UNION compares, orders and takes MIN and MAX of an enum column by declaration order"

[[test.results]]
value = "[{ name: 'a' }, { name: 'b' }]"

[[test.results]]
value = "[{ v: 'sad' }, { v: 'happy' }]"

[[test.results]]
value = "[{ name: 'b' }, { name: 'c' }]"

[[test.results]]
value = "[{ name: 'b' }, { name: 'c' }, { name: 'a' }]"

*/

SELECT name FROM person WHERE m > 'ok' UNION ALL SELECT name FROM person WHERE m < 'ok';

SELECT min(m) AS v FROM person UNION ALL SELECT max(m) AS v FROM person;

SELECT name FROM person WHERE m BETWEEN 'sad' AND 'ok' UNION SELECT name FROM person WHERE name = 'z' ORDER BY name;

SELECT name FROM (SELECT name, m FROM person WHERE m <= 'ok' UNION ALL SELECT name, m FROM person WHERE m > 'ok') AS p ORDER BY m;
