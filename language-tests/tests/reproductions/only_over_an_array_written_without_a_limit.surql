/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
`ONLY` over an array source written without a limit of one is answered from the
rows there actually were: a second row is an error, and one row or none is fine
once a `LIMIT 1` is written. `ONLY [1]` plainly has one row and `ONLY []` none,
so neither is refused merely because an array *could* hold several.

Refusing by count means the statement errors exactly when it really did answer
with more than one row. A parameter makes any up-front refusal unreachable
anyway: `$empty` holding `[]` is substituted into the plan as the literal `[]`
and cannot be told apart from one written there.
"""

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"
*/

-- An array of one row, and of none, with no limit written.
SELECT * FROM ONLY [1];
SELECT * FROM ONLY [];

-- A limit of one settles it on either engine.
SELECT * FROM ONLY [1] LIMIT 1;
SELECT * FROM ONLY [] LIMIT 1;

-- So does a source that is not an array.
SELECT * FROM ONLY 1;
SELECT * FROM ONLY NONE;

-- And a second row is an error either way.
SELECT * FROM ONLY [1, 2];
