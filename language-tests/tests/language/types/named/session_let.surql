/**
[env]
namespace = true
database = true

[test]
reason = "A top-level LET of a named type coerces the value to the type's body and runs its checks, as a block-scoped LET does"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Tried to set `$x`, but couldn't coerce value: Expected `pos_int (int)` but found `'not_an_int'`"

[[test.results]]
error = "Tried to set `$y`, but couldn't coerce value: Expected `pos_int (ASSERT $value > 0 failed)` but found `-1`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "5"

[[test.results]]
error = "Tried to set `$w`, but couldn't coerce value: Expected `pos_int (ASSERT $value > 0 failed)` but found `-1`"
*/

DEFINE TYPE pos_int AS int ASSERT $value > 0;
LET $x: pos_int = 'not_an_int';
LET $y: pos_int = -1;
LET $z: pos_int = 5.0;
RETURN $z;
{ LET $w: pos_int = -1; RETURN $w; };
