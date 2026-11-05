/**
[test]
reason = "Tests type::number casting preserving int/float/decimal variants and parsing numeric strings"

[[test.results]]
value = "13"

[[test.results]]
value = "13.5f"

[[test.results]]
value = "2.5dec"

[[test.results]]
value = "13"

[[test.results]]
value = "13.5f"

[[test.results]]
value = "100f"

[[test.results]]
error = "Could not cast into `number` using input `'abc'`"

[[test.results]]
error = "Could not cast into `number` using input `true`"

[[test.results]]
error = "Could not cast into `number` using input `NONE`"
*/

type::number(13);
type::number(13.5);
type::number(2.5dec);
type::number("13");
type::number("13.5");
type::number("1e2");
type::number("abc");
type::number(true);
type::number(NONE);
