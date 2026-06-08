/**
[test]

[[test.results]]
value = "3h"

[[test.results]]
value = "2d2h"

[[test.results]]
value = "2d2h"

[[test.results]]
error = 'Failed to compute: "duration::from_hours(9223372036854775807)", as the operation results in an arithmetic overflow.'

[[test.results]]
error = 'Failed to compute: "duration::from_hours(-1)", as the operation results in a negative value.'

*/
duration::from_hours(3);
duration::from_hours(50);
duration::from_hours(50);
duration::from_hours(9_223_372_036_854_775_807);
duration::from_hours(-1);
