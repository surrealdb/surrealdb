/**
[test]
reason = "A third argument of any type is rendered into the value::expect failure message in its SurrealQL form, so quoting stays balanced and control characters stay escaped whatever the message contains. NONE is treated as no message at all and leaves the bare assertion-failed error, whether it is written out, reaches the call through a variable, or is omitted; NULL is a value and is rendered. The argument is never inspected while the assertion holds."

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: 'too small'"

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: ''"

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: \"it's small\""

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: 'too\\nsmall'"

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: 1"

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: 1.5f"

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: 1s"

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: true"

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: { code: 400, msg: 'too small' }"

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: ['too', 'small']"

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: NULL"

[[test.results]]
error = "An error occurred: value::expect assertion failed"

[[test.results]]
value = "NONE"

[[test.results]]
error = "An error occurred: value::expect assertion failed"

[[test.results]]
error = "An error occurred: value::expect assertion failed"

[[test.results]]
value = "10"

[[test.results]]
value = "10"
*/

-- A string message is quoted into the error, empty string included
RETURN value::expect(10, |$v| $v > 50, "too small");
RETURN value::expect(10, |$v| $v > 50, "");

-- Quoting adapts to the contents so it stays balanced and escaped
RETURN value::expect(10, |$v| $v > 50, "it's small");
RETURN value::expect(10, |$v| $v > 50, "too\nsmall");

-- Every other type is rendered in its SurrealQL form
RETURN value::expect(10, |$v| $v > 50, 1);
RETURN value::expect(10, |$v| $v > 50, 1.5f);
RETURN value::expect(10, |$v| $v > 50, <duration> "1s");
RETURN value::expect(10, |$v| $v > 50, true);
RETURN value::expect(10, |$v| $v > 50, { msg: "too small", code: 400 });
RETURN value::expect(10, |$v| $v > 50, ["too", "small"]);
RETURN value::expect(10, |$v| $v > 50, NULL);

-- NONE counts as no message, whether written out, held in a variable, or omitted
RETURN value::expect(10, |$v| $v > 50, NONE);
LET $unset = NONE;
RETURN value::expect(10, |$v| $v > 50, $unset);
RETURN value::expect(10, |$v| $v > 50);

-- The message argument is irrelevant while the assertion holds
RETURN value::expect(10, |$v| $v > 5, NONE);
RETURN value::expect(10, |$v| $v > 5, 1);
