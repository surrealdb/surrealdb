/**
[test]

[[test.results]]
match = "$result == math::inf"

[[test.results]]
value = "101"

[[test.results]]
value = "101.5f"

*/
math::min([]);
math::min([101, 213, 202]);
math::min([101.5, 213.5, 202.5]);
