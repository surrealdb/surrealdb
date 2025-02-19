/**
[test]

[[test.results]]
value = "3h"

[[test.results]]
value = "2d2h"

[[test.results]]
value = "2d2h"

[[test.results]]
error = 'Failed to compute: "duration::from::hours(9223372036854775807)", as the operation results in an arithmetic overflow.'

[[test.results]]
error = 'Failed to compute: "duration::from::hours(18446744073709551615)", as the operation results in an arithmetic overflow.'

*/
duration::from::hours(3);
duration::from::hours(50);
duration::from::hours(50);
duration::from::hours(9_223_372_036_854_775_807);
duration::from::hours(-1);
