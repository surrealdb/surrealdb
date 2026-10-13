/**
[env]
namespace = true
database = true

[test]
reason = """
AND and OR return the operand that decides the result, not a boolean. Multiplying by an
integer zero keeps the numeric type and the sign of the other operand, `math::inf * 0` is NaN, and
a NONE operand raises.
"""

[[test.results]]
value = "[0, '', NONE, 1, 'a']"

[[test.results]]
value = "['x', 'y', false]"

[[test.results]]
error = "Cannot perform multiplication with 'none' and 'int'"

[[test.results]]
value = "0f"

[[test.results]]
value = "-0f"

[[test.results]]
value = "0dec"

[[test.results]]
value = "NaN"

[[test.results]]
value = "[true, 'float', 'decimal']"
*/

RETURN [0 AND false, '' AND false, NONE AND false, 1 OR true, 'a' OR true];
RETURN [1 AND 'x', 0 OR 'y', 0 OR false];
RETURN [1.5 * 0, 1dec * 0, math::inf * 0, NONE * 0];
RETURN 1.5 * 0;
RETURN -1.5 * 0;
RETURN 1dec * 0;
RETURN math::inf * 0;
RETURN [1.5 * 0 = 0, type::of(1.5 * 0), type::of(1dec * 0)];
