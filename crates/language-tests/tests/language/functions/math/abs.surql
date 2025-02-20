/**
[test]

[[test.results]]
value = "0"

[[test.results]]
value = "100"

[[test.results]]
value = "100"

[[test.results]]
error = 'Failed to compute: "math::abs(-9223372036854775808)", as the operation results in an arithmetic overflow.'

*/
math::abs(0);
math::abs(100);
math::abs(-100);
math::abs(-9_223_372_036_854_775_808);
