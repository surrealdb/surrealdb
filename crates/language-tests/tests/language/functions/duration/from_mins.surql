/**
[test]

[[test.results]]
value = "3m"

[[test.results]]
value = "1h40m"

[[test.results]]
error = 'Failed to compute: "duration::from::mins(9223372036854775807)", as the operation results in an arithmetic overflow.'

[[test.results]]
error = 'Failed to compute: "duration::from::mins(18446744073709551615)", as the operation results in an arithmetic overflow.'

*/
duration::from::mins(3);
duration::from::mins(100);
duration::from::mins(9_223_372_036_854_775_807);
duration::from::mins(-1);
