/**
[test]
reason = "Tests type::duration casting from durations, parseable strings, and invalid inputs"

[[test.results]]
value = "1h30m"

[[test.results]]
value = "1h30m"

[[test.results]]
value = "3w"

[[test.results]]
value = "1s500ms"

[[test.results]]
error = "Could not cast into `duration` using input `'abc'`"

[[test.results]]
error = "Could not cast into `duration` using input `60`"

[[test.results]]
error = "Could not cast into `duration` using input `NONE`"
*/

type::duration(1h30m);
type::duration("90m");
type::duration("3w");
type::duration("1s500ms");
type::duration("abc");
type::duration(60);
type::duration(NONE);
