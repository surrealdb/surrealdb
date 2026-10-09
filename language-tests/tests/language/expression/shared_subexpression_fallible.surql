/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["language/expression/shared_subexpression_fallible_import.surql"]

[test]
reason = "A fallible pure expression an evaluation region repeats is evaluated once where its first occurrence runs unconditionally, and raises there with the message and at the point the unshared statement raises it. A first occurrence under a short circuit that does not evaluate it is not the one a later unconditional occurrence reads: that occurrence raises for itself. One pair of statements for each kind of fallible expression: a cast, a negation, a builtin's argument, an index step and a user function."

[[test.results]]
value = "[{ arr: [1, 2], id: t:1, w: 'x' }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Cannot negate the value 'string'"

[[test.results]]
error = "Cannot negate the value 'string'"

[[test.results]]
error = "Incorrect arguments for function math::sqrt(). Argument 1 was the wrong type. Expected `number` but found `'x'`"

[[test.results]]
error = "Incorrect arguments for function math::sqrt(). Argument 1 was the wrong type. Expected `number` but found `'x'`"

[[test.results]]
error = "Invalid range: Expected `int` but found `'a'` when coercing an element of `range<int>`"

[[test.results]]
error = "Invalid range: Expected `int` but found `'a'` when coercing an element of `range<int>`"

[[test.results]]
error = "Incorrect arguments for function fn::f(). Failed to coerce argument `$x`: Expected `int` but found `'x'`"

[[test.results]]
value = "[{ x: 2, y: 2 }]"
*/

CREATE t:1 SET w = 'x', arr = [1, 2];

-- A cast: the first occurrence raises; one under a short circuit that skips it is not read.
SELECT <int> w AS x, <int> w + 1 AS y FROM t;
SELECT w = 'q' AND <int> w = 1 AS x, <int> w + 0 AS y FROM t;

-- A negation.
SELECT -w AS x, -w + 0 AS y FROM t;
SELECT w = 'q' AND -w = 1 AS x, -w + 0 AS y FROM t;

-- A builtin whose argument is of the wrong type.
SELECT math::sqrt(w) AS x, math::sqrt(w) + 0 AS y FROM t;
SELECT w = 'q' AND math::sqrt(w) = 1 AS x, math::sqrt(w) + 0 AS y FROM t;

-- An index step that cannot slice.
SELECT arr['a'..'b'] AS x, (arr['a'..'b']) + 0 AS y FROM t;
SELECT w = 'q' AND arr['a'..'b'] = [1] AS x, (arr['a'..'b']) + 0 AS y FROM t;

-- A user function, whose body the call site does not see, is never shared.
SELECT fn::f(w) + 0 AS x, fn::f(w) + 0 AS y FROM t;
SELECT fn::f(1) + 0 AS x, fn::f(1) + 0 AS y FROM t;
