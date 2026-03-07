/**
[test]
reason = "Test time::from_millis() converts milliseconds to datetime"

[[test.results]]
value = "d'1970-01-01T00:00:00Z'"

[[test.results]]
value = "d'2024-06-15T12:30:00Z'"

*/
time::from_millis(0);
time::from_millis(1718454600000);
