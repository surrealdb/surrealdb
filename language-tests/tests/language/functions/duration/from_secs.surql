/**
[test]

[[test.results]]
value = "3s"

[[test.results]]
value = "1m40s"

[[test.results]]
value = "292471208677y27w6d15h30m7s"

[[test.results]]
error = 'Failed to compute: "duration::from_secs(-1)", as the operation results in a negative value.'

*/
duration::from_secs(3);
duration::from_secs(100);
duration::from_secs(9_223_372_036_854_775_807);
duration::from_secs(-1);
