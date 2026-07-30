/**
[test]
reason = "value::expect() called as a path function returns its first argument unchanged when the assertion closure returns true, and throws otherwise; the error names the full `value::expect` path on every planner strategy"

[[test.results]]
value = "10"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "11"

[[test.results]]
value = "3"

[[test.results]]
value = "[10, 20]"

[[test.results]]
value = "'ok'"

[[test.results]]
error = "An error occurred: value::expect assertion failed"

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: 'too small'"

[[test.results]]
error = "An error occurred: value::expect assertion failed"

[[test.results]]
error = "Incorrect arguments for function value::expect(). Expected 2 to 3 arguments"

[[test.results]]
error = "Incorrect arguments for function value::expect(). Expected 2 to 3 arguments"

[[test.results]]
error = "Incorrect arguments for function value::expect(). Expected 2 to 3 arguments"

[[test.results]]
error = "Incorrect arguments for function value::expect(). Argument 2 was the wrong type. Expected `function` but found `'not a closure'`"

[[test.results]]
error = "Incorrect arguments for function value::expect(). Assertion closure must return a bool, got int"

[[test.results]]
error = "Incorrect arguments for function value::expect(). Assertion closure must return a bool, got none"

[[test.results]]
error = "Incorrect arguments for function value::expect(). Assertion closure must return a bool, got string"
*/

-- A satisfied assertion yields the original value, unmodified and unwrapped
RETURN value::expect(10, |$v| $v > 5);
RETURN value::expect([1, 2, 3], |$v| $v.len() = 3);
RETURN value::expect({ a: 1 }, |$v| $v.a = 1).a;
RETURN value::expect(NONE, |$v| $v = NONE);

-- The call is an ordinary expression and composes with the surrounding one
RETURN 1 + value::expect(10, |$v| $v > 5);
RETURN value::expect(1, |$v| $v > 0) + value::expect(2, |$v| $v > 0);
RETURN value::expect([1, 2], |$v| $v.all(|$n| $n > 0)).map(|$n| $n * 10);

-- The assertion only runs when the branch holding it is taken
RETURN IF false { value::expect(10, |$v| false) } ELSE { "ok" };

-- A false assertion throws, optionally quoting the supplied message
RETURN value::expect(10, |$v| $v > 50);
RETURN value::expect(10, |$v| $v > 50, "too small");
RETURN value::expect(10, |$v| type::is_bool($v));

-- Arity and argument-type validation
RETURN value::expect(10);
RETURN value::expect();
RETURN value::expect(10, |$v| $v > 5, "a", "b");
RETURN value::expect(10, "not a closure");

-- The closure return value must be a bool; truthy values are not coerced
RETURN value::expect(10, |$v| $v);
RETURN value::expect(10, |$v| NONE);
RETURN value::expect(10, |$v| "yes");
