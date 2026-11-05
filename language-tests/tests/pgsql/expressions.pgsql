/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "PostgreSQL arithmetic and string concatenation expressions"

[[test.results]]
value = "[{ greeting: 'hello world', res1: 7, res2: 2, res3: 2 }]"
*/

SELECT 1 + 2 * 3 AS res1, (10 - 2) / 4 AS res2, 17 % 5 AS res3, 'hello ' || 'world' AS greeting;
