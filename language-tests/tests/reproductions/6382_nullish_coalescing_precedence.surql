/**
[test]
reason = "Ensure ?? and ?: have lower precedence than arithmetic operators"
issue = 6382

[[test.results]]
value = "2147483647"

[[test.results]]
value = "20"

[[test.results]]
value = "2147483647"

[[test.results]]
value = "20"

[[test.results]]
value = "7"

[[test.results]]
value = "3"

[[test.results]]
value = "7"

[[test.results]]
value = "3"

*/

-- ?? should have lower precedence than ** and -
RETURN NONE ?? 2 ** 31 - 1;

-- When lhs is not nullish, ?? returns lhs unchanged
RETURN 20 ?? 2 ** 31 - 1;

-- ?: should have lower precedence than ** and -
RETURN false ?: 2 ** 31 - 1;

-- When lhs is truthy, ?: returns lhs unchanged
RETURN 20 ?: 2 ** 31 - 1;

-- ?? should have lower precedence than + and *
RETURN NONE ?? 3 + 4;

-- When lhs is not nullish, ?? returns lhs
RETURN 3 ?? 3 + 4;

-- ?: should have lower precedence than + and *
RETURN false ?: 3 + 4;

-- When lhs is truthy, ?: returns lhs
RETURN 3 ?: 3 + 4;
