/**
[env.capabilities]
deny-functions = ["string::len", "array::map", "array::filter", "math::abs", "fn::denied"]

[test]
reason = """
A denied function is refused by every form that reaches it, not only by a
direct call: a method resolves to the builtin a direct call names and is
refused under that name, a builtin taking a closure is refused before it runs
the closure, a builtin called in a closure's body is refused when the body
runs, and a builtin called in a user-defined function's body is refused however
the body is run. A user-defined function that is itself denied is refused
before its body is reached.

Controls: a method whose receiver resolves to a builtin that is not denied
(`array::len` for an array receiver) runs, as does a closure-taking builtin
whose closure calls nothing denied — so a refusal here is the capability and
not the shape of the call.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, s: 'abcd', xs: [1, -2] }]"

[[test.results]]
error = "Function 'string::len' is not allowed to be executed"

[[test.results]]
error = "Function 'string::len' is not allowed to be executed"

[[test.results]]
error = "Function 'string::len' is not allowed to be executed"

[[test.results]]
error = "Function 'string::len' is not allowed to be executed"

[[test.results]]
value = "2"

[[test.results]]
error = "Function 'array::map' is not allowed to be executed"

[[test.results]]
error = "Function 'array::map' is not allowed to be executed"

[[test.results]]
error = "Function 'array::filter' is not allowed to be executed"

[[test.results]]
error = "Function 'array::filter' is not allowed to be executed"

[[test.results]]
error = "Function 'math::abs' is not allowed to be executed"

[[test.results]]
error = "Function 'math::abs' is not allowed to be executed"

[[test.results]]
error = "Function 'math::abs' is not allowed to be executed"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Function 'math::abs' is not allowed to be executed"

[[test.results]]
error = "Function 'math::abs' is not allowed to be executed"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Function 'string::len' is not allowed to be executed"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Function 'string::len' is not allowed to be executed"

[[test.results]]
error = "Function 'string::len' is not allowed to be executed"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Function 'fn::denied' is not allowed to be executed"

[[test.results]]
error = "Function 'fn::denied' is not allowed to be executed"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET s = 'abcd', xs = [1, -2];

-- Methods: a bare receiver, a field receiver, and a method in a WHERE.
RETURN string::len('abcd');
RETURN 'abcd'.len();
SELECT s.len() AS n FROM t;
SELECT * FROM t WHERE s.len() = 4;
RETURN [1, 2].len();

-- A closure-taking builtin, by call and by method.
RETURN array::map([1, 2], |$x| $x);
RETURN [1, 2].map(|$x| $x);
RETURN array::filter([1, 2], |$x| $x > 1);
RETURN [1, 2].filter(|$x| $x > 1);

-- A denied builtin in the body of a closure an allowed builtin runs.
RETURN array::any([1, -2], |$x| math::abs($x) > 1);
SELECT array::any(xs, |$x| math::abs($x) > 1) AS a FROM t;
RETURN [1, -2].any(|$x| math::abs($x) > 1);
RETURN array::any([1, -2], |$x| $x > 0);

-- A denied builtin, and a denied method, in a user-defined function's body
-- small enough to run in place of the call.
DEFINE FUNCTION fn::abs($x: number) { RETURN math::abs($x) };
RETURN fn::abs(-1);
SELECT fn::abs(xs[1]) AS a FROM t;
DEFINE FUNCTION fn::len_method($x: string) { RETURN $x.len() };
RETURN fn::len_method('ab');
DEFINE FUNCTION fn::len_call($x: string) { RETURN string::len($x) };
RETURN fn::len_call('ab');
RETURN array::all([1], |$x| fn::len_call('ab') > 0);

-- A denied user-defined function whose body calls nothing denied.
DEFINE FUNCTION fn::denied($x: number) { RETURN $x + 1 };
RETURN fn::denied(1);
SELECT fn::denied(xs[0]) AS a FROM t;
