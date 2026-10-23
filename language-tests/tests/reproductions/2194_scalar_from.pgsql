/**
[env]
namespace = true
database = true

[test]
issue = 2194
reason = "Registered scalar functions in FROM expose a single correctly named row, including aliases and ordinality."

[[test.results]]
value = "[{ current_schema: 'public' }]"
[[test.results]]
value = "[{ a: 2 }]"
[[test.results]]
value = "[{ n: 2, ord: 1 }]"
[[test.results]]
value = "[{ n: 2, s: 'public' }]"
*/
SELECT * FROM current_schema();
SELECT * FROM abs(-2) AS a;
SELECT * FROM abs(-2) WITH ORDINALITY AS a(n, ord);
SELECT a.n, s.v AS s FROM abs(-2) AS a(n) CROSS JOIN current_schema() AS s(v);
