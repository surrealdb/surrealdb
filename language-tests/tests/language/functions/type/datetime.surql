/**
[test]
reason = "Tests type::datetime casting from datetimes, parseable strings, and invalid inputs"

[[test.results]]
value = "d'2020-01-01T00:00:00Z'"

[[test.results]]
value = "d'2020-01-01T12:34:56Z'"

[[test.results]]
value = "d'2020-01-01T12:34:56.123456789Z'"

[[test.results]]
value = "d'2019-12-31T22:00:00Z'"

[[test.results]]
error = "Could not cast into `datetime` using input `'2020-13-01T00:00:00Z'`"

[[test.results]]
error = "Could not cast into `datetime` using input `'not a datetime'`"

[[test.results]]
error = "Could not cast into `datetime` using input `123`"

[[test.results]]
error = "Could not cast into `datetime` using input `NONE`"
*/

type::datetime(d"2020-01-01T00:00:00Z");
type::datetime("2020-01-01T12:34:56Z");
type::datetime("2020-01-01T12:34:56.123456789Z");
type::datetime("2020-01-01T00:00:00+02:00");
type::datetime("2020-13-01T00:00:00Z");
type::datetime("not a datetime");
type::datetime(123);
type::datetime(NONE);
