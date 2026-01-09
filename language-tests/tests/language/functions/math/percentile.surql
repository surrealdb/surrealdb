/**
[test]

[[test.results]]
value = "NaN"

[[test.results]]
value = "212.78f"

[[test.results]]
value = "213.28f"

[[test.results]]
value = "NaN"

[[test.results]]
value = "NaN"

*/

math::percentile([], 99);
math::percentile([101, 213, 202], 99);
math::percentile([101.5, 213.5, 202.5], 99);
math::percentile([101.5, 213.5, 202.5], -1);
math::percentile([101.5, 213.5, 202.5], 101);
