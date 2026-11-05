/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL chr and ascii work on code points"

[[test.results]]
value = "[{ a: 'A', b: 65, c: 233, d: 8364, e: 0 }]"
*/

SELECT chr(65) AS a, ascii('A') AS b, ascii('é') AS c, ascii('€') AS d, ascii('') AS e;
