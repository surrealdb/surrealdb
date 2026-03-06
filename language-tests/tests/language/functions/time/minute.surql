/**
[test]
reason = "Test time::minute() extracts the minute component"

[[test.results]]
value = "0"

[[test.results]]
value = "30"

[[test.results]]
value = "59"

*/
time::minute(d"2024-01-01T00:00:00Z");
time::minute(d"2024-06-15T12:30:00Z");
time::minute(d"2024-12-31T23:59:59Z");
