/**
[env]
namespace = true
database = true

[test]
reason = "A float outside the i64 range is refused when it is coerced or cast to an int, never clamped to i64::MAX or i64::MIN. The float is whole, so a check that it has no fraction does not catch it."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `n` of `t:1`: Expected `int` but found `1000000000000000000000000000000f`"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `1000000000000000000000000000000f`"

[[test.results]]
error = "Could not cast into `int` using input `-1000000000000000000000000000000f`"

[[test.results]]
error = "Could not cast into `int` using input `9223372036854776000f`"

[[test.results]]
error = "Could not cast into `int` using input `1000000000000000000000000000000f`"
*/

DEFINE FIELD n ON t TYPE int;
CREATE t:1 SET n = 1e30;
-- Nothing was stored
(SELECT VALUE n FROM t:1);
<int>1e30;
<int>-1e30;
-- 2^63 is the first whole float above i64::MAX
<int>9.223372036854775807e18;
type::int(1e30);
