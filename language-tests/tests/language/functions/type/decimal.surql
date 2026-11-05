/**
[test]
reason = "Tests type::decimal casting from decimals, ints, floats, numeric strings, and invalid inputs"

[[test.results]]
value = "1.5dec"

[[test.results]]
value = "13dec"

[[test.results]]
value = "0.1dec"

[[test.results]]
value = "13.5719dec"

[[test.results]]
value = "1000dec"

[[test.results]]
value = "-0.25dec"

[[test.results]]
error = "Could not cast into `decimal` using input `'abc'`"

[[test.results]]
error = "Could not cast into `decimal` using input `true`"

[[test.results]]
error = "Could not cast into `decimal` using input `NONE`"
*/

type::decimal(1.5dec);
type::decimal(13);
type::decimal(0.1);
type::decimal("13.5719");
type::decimal("1e3");
type::decimal("-0.25");
type::decimal("abc");
type::decimal(true);
type::decimal(NONE);
