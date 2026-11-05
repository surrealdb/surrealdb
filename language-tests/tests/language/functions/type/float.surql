/**
[test]
reason = "Tests type::float casting from floats, ints, decimals, numeric strings, and invalid inputs"

[[test.results]]
value = "13.5f"

[[test.results]]
value = "7f"

[[test.results]]
value = "2.5f"

[[test.results]]
value = "13.571938471f"

[[test.results]]
value = "100f"

[[test.results]]
value = "NaN"

[[test.results]]
error = "Could not cast into `float` using input `'abc'`"

[[test.results]]
error = "Could not cast into `float` using input `true`"

[[test.results]]
error = "Could not cast into `float` using input `NONE`"
*/

type::float(13.5);
type::float(7);
type::float(2.5dec);
type::float("13.571938471");
type::float("1e2");
type::float("NaN");
type::float("abc");
type::float(true);
type::float(NONE);
