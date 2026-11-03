/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL mod, sign, pi, degrees, radians, exp, ln, log and round(x, n)"

[[test.results]]
value = "[{ a: 1, b: -1, c: 1.5f, d: NULL }]"

[[test.results]]
value = "[{ a: -1, b: 0, c: 1f }]"

[[test.results]]
value = "[{ deg: 180f, pi: true, rad: true }]"

[[test.results]]
value = "[{ a: 1f, b: true, c: 0f, d: true }]"

[[test.results]]
value = "[{ base10: true, base2: true, n: NULL }]"

[[test.results]]
value = "[{ a: 2.57dec, b: 3dec, c: -3dec, d: 1200dec }]"

*/

SELECT mod(10, 3) AS a, mod(-7, 3) AS b, mod(7.5, 2) AS c, mod(NULL, 2) AS d;

SELECT sign(-5) AS a, sign(0) AS b, sign(2.5) AS c;

SELECT pi() > 3.14 AND pi() < 3.15 AS pi, degrees(3.141592653589793) AS deg, abs(radians(180) - pi()) < 0.000000001 AS rad;

SELECT exp(0) AS a, abs(exp(1) - 2.718281828) < 0.000001 AS b, ln(1) AS c, abs(ln(10) - 2.302585093) < 0.000001 AS d;

SELECT abs(log(100) - 2) < 0.000001 AS base10, abs(log(2, 8) - 3) < 0.000001 AS base2, log(NULL) AS n;

SELECT round(2.567, 2) AS a, round(2.5) AS b, round(-2.5, 0) AS c, round(1234.5, -2) AS d;
