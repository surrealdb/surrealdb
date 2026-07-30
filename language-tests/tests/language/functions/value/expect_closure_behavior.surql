/**
[test]
reason = "value::expect applies ordinary closure-invocation rules to its assertion closure: the parameter count is not checked against the single value passed in, so surplus parameters bind to NONE and a zero-parameter closure simply ignores the value. A declared parameter type is enforced by the closure itself and reports the closure's own coercion error, and an error raised inside the closure propagates verbatim instead of being reported as a failed assertion."

[[test.results]]
value = "10"

[[test.results]]
value = "10"

[[test.results]]
value = "10"

[[test.results]]
error = "An error occurred: value::expect assertion failed"

[[test.results]]
value = "10"

[[test.results]]
value = "10"

[[test.results]]
error = "Incorrect arguments for function ANONYMOUS(). Expected a value of type 'string' for argument $v"

[[test.results]]
error = "An error occurred: boom"
*/

-- The closure arity is not validated against the asserted value
RETURN value::expect(10, || true);
RETURN value::expect(10, |$a, $b| true);
RETURN value::expect(10, |$a, $b| $b = NONE);
RETURN value::expect(10, || false);

-- A block body is accepted as long as it evaluates to a bool
RETURN value::expect(10, |$v| IF $v > 5 { true } ELSE { false });

-- A declared parameter type is enforced by the closure, not by value::expect
RETURN value::expect(10, |$v: int| $v > 5);
RETURN value::expect(10, |$v: string| $v = "x");

-- Errors raised inside the closure surface unchanged
RETURN value::expect(10, |$v| THROW "boom");
