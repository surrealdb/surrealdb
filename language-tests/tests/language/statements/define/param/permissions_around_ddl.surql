/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A parameter with a conditional `PERMISSIONS` clause read before and after a statement of the same transaction alters, redefines or removes it answers the definition each read finds; so does a function redefined, removed and called in one block."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['first', 'second']"

[[test.results]]
value = "[['second', 'one'], ['second', 'two']]"

[[test.results]]
value = "['second', NONE]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "'two'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['in', 'out']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'out'"
*/

DEFINE PARAM $gate VALUE 'first' PERMISSIONS WHERE $auth.id = NONE OR true;
DEFINE FUNCTION fn::label() { RETURN 'one' } PERMISSIONS WHERE true;
{ LET $a = $gate; ALTER PARAM $gate VALUE 'second' PERMISSIONS WHERE false; RETURN [$a, $gate] };
{ LET $a = [$gate, fn::label()]; DEFINE FUNCTION OVERWRITE fn::label() { RETURN 'two' } PERMISSIONS WHERE 1 = 1; RETURN [$a, [$gate, fn::label()]] };
{ LET $a = $gate; REMOVE PARAM $gate; RETURN [$a, $gate] };
{ DEFINE PARAM $made VALUE 1 PERMISSIONS WHERE true; LET $a = $made; ALTER PARAM $made VALUE 2; RETURN [$a, $made] };
{ LET $a = fn::label(); REMOVE FUNCTION fn::label; RETURN $a };
BEGIN;
DEFINE PARAM $inside VALUE 'in' PERMISSIONS WHERE $auth.id = NONE OR true;
LET $first = $inside;
ALTER PARAM $inside VALUE 'out' PERMISSIONS FULL;
RETURN [$first, $inside];
COMMIT;
$inside;
