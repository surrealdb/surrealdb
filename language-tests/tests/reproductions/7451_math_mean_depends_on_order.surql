/**
[env]
timeout = 5000

[test]
reason = "math::mean of the same multiset must not depend on element order"
issue = 7451

[[test.results]]
value = "256.75f"

[[test.results]]
value = "256.75f"

[[test.results]]
value = "6148914691236517000f"

[[test.results]]
value = "6148914691236517000f"

[[test.results]]
value = "6148914691236517000f"

[[test.results]]
value = "true"

*/

RETURN math::mean([4611686018427387904, -4611686018427387904, 27, 1000]);
RETURN math::mean([27, 1000, 4611686018427387904, -4611686018427387904]);
-- A float among integers that overflow i64 makes a float mean in any order.
RETURN math::mean([9223372036854775807, 9223372036854775807, 1.0f]);
RETURN math::mean([1.0f, 9223372036854775807, 9223372036854775807]);
RETURN math::mean([9223372036854775807, 1.0f, 9223372036854775807]);
RETURN type::is_float(math::mean([9223372036854775807, 9223372036854775807, 1]));
