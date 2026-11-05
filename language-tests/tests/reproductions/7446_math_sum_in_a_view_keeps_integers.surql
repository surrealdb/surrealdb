/**
[env]
timeout = 5000

[test]
reason = "math::sum over integers in an aggregate view is exact and stays an integer, as ad hoc"
issue = 7446

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: 'a', id: person:n0, v: 4611686018427387904 }]"

[[test.results]]
value = "[{ g: 'a', id: person:n1, v: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: 'a', int: true, total: 4611686018427387905 }]"

[[test.results]]
value = "[{ g: 'a', int: true, total: 4611686018427387905 }]"

[[test.results]]
value = "[{ g: 'a', id: person:n2, v: 2 }]"

[[test.results]]
value = "[{ g: 'a', int: true, total: 4611686018427387907 }]"

*/

DEFINE TABLE person SCHEMALESS;
CREATE person:n0 SET g = 'a', v = 4611686018427387904;
CREATE person:n1 SET g = 'a', v = 1;
DEFINE TABLE stats AS SELECT g, math::sum(v) AS total FROM person GROUP BY g;
SELECT g, total, type::is_int(total) AS int FROM stats;
SELECT g, math::sum(v) AS total, type::is_int(math::sum(v)) AS int FROM person GROUP BY g;
CREATE person:n2 SET g = 'a', v = 2;
SELECT g, total, type::is_int(total) AS int FROM stats;
