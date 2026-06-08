/**
[test]
reason = "Ensure value::diff/value::patch round-trip when both operands are top-level scalars"
issue = 7239

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ msg: 'Hello there!' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'Hello there!'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

*/

-- Object round-trip already works.
LET $start = { msg: "Hello" };
LET $after = { msg: "Hello there!" };
LET $diff = value::diff($start, $after);
RETURN value::patch($start, $diff);

-- String root round-trip — was silently no-op before fix.
LET $start = "Hello";
LET $after = "Hello there!";
LET $diff = value::diff($start, $after);
RETURN value::patch($start, $diff);

-- Number root round-trip — uses Operation::Replace at the root.
LET $start = 1;
LET $after = 2;
LET $diff = value::diff($start, $after);
RETURN value::patch($start, $diff);
