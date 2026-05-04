/**
[test]
issue = 7239
reason = "Root-level scalars: value::patch($start, value::diff($start, $after)) must yield $after (Change for strings, Replace for other scalars and type changes). Covers RFC 6901 path '' vs '/', scalar root diff using path [], and objects with an empty-string key."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'Hello there!'"

[[test.results]]
value = "99"

[[test.results]]
value = "false"

[[test.results]]
value = "42"

[[test.results]]
value = "'something'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ '': 'world', x: 1 }"

*/

LET $start = 'Hello';
LET $after = 'Hello there!';
LET $diff = value::diff($start, $after);
RETURN value::patch($start, $diff);
RETURN value::patch(42, value::diff(42, 99));
RETURN value::patch(true, value::diff(true, false));
RETURN value::patch('hello', value::diff('hello', 42));
RETURN value::patch(NULL, value::diff(NULL, 'something'));
LET $a = { '': 'hello', x: 1 };
LET $b = { '': 'world', x: 1 };
RETURN value::patch($a, value::diff($a, $b));
