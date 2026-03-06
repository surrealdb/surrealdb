/**
[test]
reason = "Test time::millis() returns milliseconds since Unix epoch"

[[test.results]]
value = "0"

[[test.results]]
value = "1718454600000"

*/
time::millis(d"1970-01-01T00:00:00Z");
time::millis(d"2024-06-15T12:30:00Z");
