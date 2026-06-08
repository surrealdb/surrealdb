/**
[test]

[[test.results]]
value = "3ms"

[[test.results]]
value = "50ms"

[[test.results]]
value = "292471208y35w2d7h12m55s807ms"

[[test.results]]
error = 'Failed to compute: "duration::from_millis(-1)", as the operation results in a negative value.'

*/
duration::from_millis(3);
duration::from_millis(50);
duration::from_millis(9_223_372_036_854_775_807);
duration::from_millis(-1);
