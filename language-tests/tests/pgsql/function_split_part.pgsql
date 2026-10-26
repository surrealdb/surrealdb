/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL split_part counts fields from 1, counts from the end for a negative position, and answers the empty string past the last field"

[[test.results]]
value = "[{ a: 'b', b: 'c', c: '', d: 'abc', e: NULL }]"
*/

SELECT split_part('a,b,c', ',', 2) AS a, split_part('a,b,c', ',', -1) AS b, split_part('a,b,c', ',', 4) AS c, split_part('abc', '', 1) AS d, split_part(NULL, ',', 1) AS e;
