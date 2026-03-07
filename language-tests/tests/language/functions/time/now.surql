/**
[test]
reason = "Test time::now() returns a datetime"

[[test.results]]
value = "true"

*/
type::is_datetime(time::now());
