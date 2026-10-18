/**
[env]
timeout = 5000

[test]
reason = "math::mean over integers must not lose precision accumulating in f64"
issue = 7450

[[test.results]]
value = "149.5f"

[[test.results]]
value = "[{ id: n:1, v: 4611686018427387904 }]"

[[test.results]]
value = "[{ id: n:2, v: 4611686018427387904 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ avg: 4611686018427388000f, avg_float: true }]"

[[test.results]]
value = "[{ id: n:3, v: -4611686018427387904 }]"

[[test.results]]
value = "[{ id: n:4, v: -4611686018427387904 }]"

[[test.results]]
value = "[{ id: n:5, v: 299 }]"

[[test.results]]
value = "[{ id: n:6, v: 299 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ avg: 149.5f, avg_float: true }]"

*/

RETURN math::mean([299, -4611686018427387904, 299, 4611686018427387904]);
CREATE n:1 SET v = 4611686018427387904;
CREATE n:2 SET v = 4611686018427387904;
DEFINE TABLE m AS SELECT math::mean(v) AS avg FROM n GROUP ALL;
SELECT avg, type::is_float(avg) AS avg_float FROM m;
CREATE n:3 SET v = -4611686018427387904;
CREATE n:4 SET v = -4611686018427387904;
CREATE n:5 SET v = 299;
CREATE n:6 SET v = 299;
DELETE n:1;
DELETE n:3;
SELECT avg, type::is_float(avg) AS avg_float FROM m;
