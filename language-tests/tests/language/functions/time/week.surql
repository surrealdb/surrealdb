/**
[test]
reason = "Test time::week() returns the ISO week number"

[[test.results]]
value = "1"

[[test.results]]
value = "24"

*/
time::week(d"2024-01-01T00:00:00Z");
time::week(d"2024-06-15T12:00:00Z");
