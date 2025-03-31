/**
[test]

[[test.results]]
value = "3w"

[[test.results]]
value = "1y7w6d"

[[test.results]]
error = 'Failed to compute: "duration::from::weeks(9223372036854775807)", as the operation results in an arithmetic overflow.'

[[test.results]]
error = 'Failed to compute: "duration::from::weeks(18446744073709551615)", as the operation results in an arithmetic overflow.'

*/
RETURN duration::from::weeks(3);
RETURN duration::from::weeks(60);
RETURN duration::from::weeks(9_223_372_036_854_775_807);
RETURN duration::from::weeks(-1);
