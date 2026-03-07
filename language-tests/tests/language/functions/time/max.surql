/**
[test]
reason = "Test time::max() returns the latest datetime from an array"

[[test.results]]
value = "d'2024-06-15T12:00:00Z'"

[[test.results]]
value = "NONE"

*/
time::max([d"2024-06-15T12:00:00Z", d"2020-01-01T00:00:00Z", d"2022-03-10T08:00:00Z"]);
time::max([]);
