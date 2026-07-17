/**
[test]
reason = "Tests type::bool casting from booleans, exact 'true'/'false' strings, and invalid inputs"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
error = "Could not cast into `bool` using input `'TRUE'`"

[[test.results]]
error = "Could not cast into `bool` using input `1`"

[[test.results]]
error = "Could not cast into `bool` using input `NULL`"
*/

type::bool(true);
type::bool(false);
type::bool("true");
type::bool("false");
type::bool("TRUE");
type::bool(1);
type::bool(NULL);
