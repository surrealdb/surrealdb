/**
[test]

[[test.results]]
value = "NaN"

[[test.results]]
value = "213"

[[test.results]]
value = "213.5f"

*/
math::nearestrank([], 75);
math::nearestrank([101, 213, 202], 75);
math::nearestrank([101.5, 213.5, 202.5], 75);
