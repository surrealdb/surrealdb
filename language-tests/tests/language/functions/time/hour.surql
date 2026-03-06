/**
[test]
reason = "Test time::hour() extracts the hour component"

[[test.results]]
value = "0"

[[test.results]]
value = "12"

[[test.results]]
value = "23"

*/
time::hour(d"2024-01-01T00:00:00Z");
time::hour(d"2024-06-15T12:30:00Z");
time::hour(d"2024-12-31T23:59:59Z");
