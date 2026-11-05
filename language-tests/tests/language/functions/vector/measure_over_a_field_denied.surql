/**
[env]
namespace = true
database = true

[env.capabilities]
deny-functions = ["vector::distance::euclidean"]

[test]
reason = """
A builtin the capabilities deny is refused when the first row reaches its call,
whether the call is over a field of the row and a literal or over any other
argument: a table with no row raises nothing. A query argument that reads an
undefined parameter is read as `NONE` when the first row reaches the call too,
and a table with no row raises nothing for it either.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Function 'vector::distance::euclidean' is not allowed to be executed"

[[test.results]]
error = "Function 'vector::distance::euclidean' is not allowed to be executed"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function vector::distance::manhattan(). Argument 2 was the wrong type. Expected `array<number>` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::manhattan(). Argument 2 was the wrong type. Expected `array<number>` but found `NONE`"

*/
{
	DEFINE TABLE empty;
	CREATE t:1 SET v = [3f, 4f];
	RETURN NONE;
};
SELECT vector::distance::euclidean(v, [0f, 0f]) FROM empty;
SELECT vector::distance::euclidean(v, [0f, 0f]) FROM t;
SELECT vector::distance::euclidean(v ?? NONE, [0f, 0f]) FROM t;
SELECT vector::distance::manhattan(v, $undefined) FROM empty;
SELECT vector::distance::manhattan(v, $undefined) FROM t;
SELECT vector::distance::manhattan(v ?? NONE, $undefined) FROM t;
