/**
[test]

[[test.results]]
value = "3µs"

[[test.results]]
value = "50µs"

[[test.results]]
value = "50µs"

[[test.results]]
value = "292471y10w6d4h54s775ms807µs"

[[test.results]]
value = "584942y21w5d8h1m49s551ms615µs"

*/
duration::from_micros(3);
duration::from_micros(50);
duration::from_micros(50);
duration::from_micros(9_223_372_036_854_775_807);
duration::from_micros(-1);
