/**
[env]
namespace = true
database = true

[test]
reason = """
LIMIT is evaluated before START whatever their order in the text: a failed LIMIT is
the error even when START would also fail (a failed cast in START never shows). A non-integer LIMIT
raises, and ONLY over a source of more than one row raises.
"""

[[test.results]]
error = "Found x but the LIMIT clause must evaluate to a positive integer"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"
*/

SELECT * FROM [1, 2] LIMIT "x";
SELECT * FROM ONLY [1, 2];
SELECT * FROM [1, 2] LIMIT (1 + "a") START (2 + "bb");
SELECT * FROM [1, 2] START (2 + "bb") LIMIT (1 + "a");
SELECT * FROM [1, 2] LIMIT (1 + "a") START <int>"x";
SELECT * FROM [1, 2] START <int>"x" LIMIT (1 + "a");
