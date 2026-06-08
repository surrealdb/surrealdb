/**
[test]

[[test.results]]
value = "30ns"

[[test.results]]
value = "5ms5µs"

[[test.results]]
value = "292y24w3d23h47m16s854ms775µs807ns"

[[test.results]]
error = 'Failed to compute: "duration::from_nanos(-1)", as the operation results in a negative value.'

*/
duration::from_nanos(30);
duration::from_nanos(5005000);
duration::from_nanos(9_223_372_036_854_775_807);
duration::from_nanos(-1);
