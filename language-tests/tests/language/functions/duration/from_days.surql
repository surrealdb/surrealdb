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
error = 'Failed to compute: "duration::from_days(18446744073709551615)", as the operation results in an arithmetic overflow.'

*/
duration::from_days(3);
duration::from_days(50);
duration::from_days(50);
duration::from_days(9_223_372_036_854_775_807);
duration::from_days(-1);
