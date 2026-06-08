/**
[test]

[[test.results]]
value = "3d"

[[test.results]]
value = "7w1d"

[[test.results]]
value = "7w1d"

[[test.results]]
error = 'Failed to compute: "duration::from_days(9223372036854775807)", as the operation results in an arithmetic overflow.'

[[test.results]]
error = 'Failed to compute: "duration::from_days(-1)", as the operation results in a negative value.'

*/
duration::from_days(3);
duration::from_days(50);
duration::from_days(50);
duration::from_days(9_223_372_036_854_775_807);
duration::from_days(-1);
