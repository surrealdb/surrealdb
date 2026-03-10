/**
[test]

[[test.results]]
error = "Incorrect arguments for method time::format(). `%Y-%m-%dT%H:%M:%S.%3NZ` is not a valid time formatting string"

*/
time::format(d"2012-01-01T08:00:00Z", "%Y-%m-%dT%H:%M:%S.%3NZ")
