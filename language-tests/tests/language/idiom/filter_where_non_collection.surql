/**
[env]
namespace = true
database = true

[test]
reason = """
`[WHERE ..]` selects from a collection. Applied to an array it yields the
matching elements; applied to a set it yields a set of the matching elements,
so the deduplicating type survives the filter. Applied to anything else --- a
number, a string, an object, a record id, NONE --- there is nothing to select
from and the path yields NONE, rather than promoting the single value to a
one-element collection.
"""
issue = 472

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "false"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "{2, 3}"

[[test.results]]
value = "{,}"

[[test.results]]
value = "true"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ arr: [2, 3], id: thing:1, nested: NONE, scalar: NONE }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "NONE"
*/

-- A predicate that matches is not enough to make a collection
RETURN (5)[WHERE $this > 1];
RETURN (5)[WHERE $this > 10];
RETURN ("abc")[WHERE $this = "abc"];
RETURN ({ a: 1 })[WHERE true];
RETURN (NONE)[WHERE true];
RETURN (true)[WHERE $this];
RETURN type::is_array((5)[WHERE $this > 1]);

-- Arrays filter to arrays, sets filter to sets
RETURN ([1, 2, 3])[WHERE $this > 1];
RETURN ([1, 2, 3])[WHERE $this > 10];
RETURN (<set>[1, 2, 3])[WHERE $this > 1];
RETURN (<set>[1, 2, 3])[WHERE $this > 10];
RETURN type::is_array((<set>[1, 2, 3])[WHERE $this > 1]) == false;

-- The same rules apply to document fields, in a projection and through a link
{
	CREATE thing:1 SET scalar = 7, arr = [1, 2, 3], nested = { a: 1 };
	RETURN "OK";
};
SELECT
	id,
	scalar[WHERE $this > 1] AS scalar,
	arr[WHERE $this > 1] AS arr,
	nested[WHERE true] AS nested
FROM thing;
RETURN thing:1.scalar[WHERE $this > 1];
RETURN thing:1.arr[WHERE $this > 1];
RETURN (thing:1)[WHERE true];
