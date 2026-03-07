/**
[test]
reason = "Test time::nano() returns nanoseconds since Unix epoch"

[[test.results]]
value = "0"

[[test.results]]
value = "1718454600000000000"

*/
time::nano(d"1970-01-01T00:00:00Z");
time::nano(d"2024-06-15T12:30:00Z");
