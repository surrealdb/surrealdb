/**
[env]
namespace = true
database = true

[test]
reason = "A tuple kind judges each element against the named type at its position"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Could not cast into `[pos_int, string]`: Expected `pos_int (int)` but found `'not_an_int'`"

[[test.results]]
error = "Could not cast into `[pos_int, string]`: Expected `pos_int (ASSERT $value > 0 failed)` but found `-1`"

[[test.results]]
value = "[3, 'ok']"

[[test.results]]
error = "Tried to set `$t`, but couldn't coerce value: Expected `pos_int (ASSERT $value > 0 failed)` but found `0`"
*/

DEFINE TYPE pos_int AS int ASSERT $value > 0;
RETURN <[pos_int, string]> ['not_an_int', 'ok'];
RETURN <[pos_int, string]> [-1, 'ok'];
RETURN <[pos_int, string]> ['3', 'ok'];
LET $t: [string, pos_int] = ['a', 0];
