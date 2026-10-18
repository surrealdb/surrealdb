/**
[env]
timeout = 5000

[test]
reason = "math::sum over integers must not silently wrap on i64 overflow"
issue = 7449

[[test.results]]
error = "Cannot perform addition with '4611686018427387904' and '4611686018427387904'"

[[test.results]]
value = "9223372036854775807"

[[test.results]]
value = "9223372036854775807"

[[test.results]]
value = "[{ id: n:1, v: 4611686018427387904 }]"

[[test.results]]
value = "[{ id: n:2, v: 4611686018427387904 }]"

[[test.results]]
value = "[{ id: n:3, v: 4611686018427387904 }]"

[[test.results]]
error = "Cannot perform addition with '4611686018427387904' and '4611686018427387904'"

[[test.results]]
error = "Cannot perform addition with '4611686018427387904' and '4611686018427387904'"

[[test.results]]
value = "[{ id: n:4, v: 4611686018427387904 }]"

*/

RETURN math::sum([4611686018427387904, 4611686018427387904, 4611686018427387904]);
RETURN math::sum([9223372036854775807, 0]);
RETURN math::sum([9223372036854775807, -1, 1]);
CREATE n:1 SET v = 4611686018427387904;
CREATE n:2 SET v = 4611686018427387904;
CREATE n:3 SET v = 4611686018427387904;
SELECT math::sum(v) AS total FROM n GROUP ALL;
DEFINE TABLE s AS SELECT math::sum(v) AS total FROM n GROUP ALL;
CREATE n:4 SET v = 4611686018427387904;
