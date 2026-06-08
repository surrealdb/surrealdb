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
error = 'Failed to compute: "duration::from_micros(-1)", as the operation results in a negative value.'

*/
duration::from_micros(3);
duration::from_micros(50);
duration::from_micros(50);
duration::from_micros(9_223_372_036_854_775_807);
duration::from_micros(-1);
