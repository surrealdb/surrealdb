/**
[test]
reason = "Ensure ?? and ?: have lower precedence than arithmetic operators"
issue = 6382

[[test.results]]
value = "20"

[[test.results]]
value = "20"

[[test.results]]
value = "3"

[[test.results]]
value = "3"

*/

-- With non-null/truthy lhs, ?? and ?: must return lhs, proving arithmetic
-- was grouped on the rhs (otherwise lhs ** 31 - 1 would be computed)
RETURN 20 ?? 2 ** 31 - 1;
RETURN 20 ?: 2 ** 31 - 1;

-- Same pattern with + to verify precedence over addition
RETURN 3 ?? 3 + 4;
RETURN 3 ?: 3 + 4;
