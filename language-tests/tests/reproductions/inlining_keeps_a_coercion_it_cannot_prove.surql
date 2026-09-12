/**
[env]
namespace = true
database = true

[test]
reason = """
A call coerces every argument to its declared kind before the body runs, and
that coercion can fail. Replacing the call by its body elides the coercion, so
it is only sound where the coercion is identity for the value in hand — and a
field's declared kind is not evidence of that: it is enforced on write alone,
so a value written before the declaration, or before `DEFINE FIELD OVERWRITE`
retyped the field, holds what it held.

The kind is therefore read the checked way, as cast elision reads it, and an
unproven kind is read as `any`. A parameter declared `any` coerces every value
by identity and still inlines.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function fn::seven(). Failed to coerce argument `$x`: Expected `int` but found `'7'`"

[[test.results]]
value = "[true]"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE FIELD a ON t TYPE string;
CREATE t:1 SET a = '7' RETURN NONE;
DEFINE FIELD OVERWRITE a ON t TYPE int;
DEFINE FUNCTION fn::seven($x: int) { RETURN $x = 7 };
DEFINE FUNCTION fn::seven_any($x: any) { RETURN $x = '7' };

-- The declaration says `int`; the stored value is the string it was written
-- as, and the coercion the call owes it is the one that reports the mismatch.
SELECT VALUE fn::seven(a) FROM t;

-- A parameter declared `any` coerces every value by identity, so the call is
-- still replaced by its body.
SELECT VALUE fn::seven_any(a) FROM t;
