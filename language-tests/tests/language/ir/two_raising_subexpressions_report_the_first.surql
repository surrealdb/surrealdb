/**
[env]
namespace = true
database = true

[test]
reason = """
A statement with two subexpressions that would each raise reports the error of the one evaluated
first, whether they sit in one array, one object (in the order its fields are written), one
WHERE or two arguments of a call, and the statements around it still answer their own results.
"""

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform subtraction with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform subtraction with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
value = "3"
*/

CREATE t:1;
RETURN [1 + "a", 2 - "b"];
RETURN [2 - "b", 1 + "a"];
RETURN { a: 1 + "a", b: 2 - "b" };
RETURN { b: 2 - "b", a: 1 + "a" };
SELECT * FROM t WHERE 1 + "a" = 1 AND 2 - "b" = 2;
RETURN math::max(1 + "a", 2 - "b");
RETURN 3;
