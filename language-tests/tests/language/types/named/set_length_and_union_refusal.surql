/**
[env]
namespace = true
database = true

[test]
reason = "A bounded set is measured again after the named types it holds collapse equal elements, and a union no member of which accepts a value is reported against the whole union"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `s` of `t:1`: Expected `set<int_type,2>` but found a collection of length `1`"

[[test.results]]
value = "[{ id: t:2, s: {1, 2} }]"

[[test.results]]
error = "Tried to set `$e`, but couldn't coerce value: Expected `pos_int | bool` but found `-5`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "5"
*/

DEFINE TYPE int_type AS int;
DEFINE TYPE pos_int AS int ASSERT $value > 0;
DEFINE FIELD s ON t TYPE set<int_type, 2>;
CREATE t:1 SET s = [1, 1.0];
CREATE t:2 SET s = [1, 2.0];
LET $e: pos_int | bool = -5;
LET $f: pos_int | bool = 5;
RETURN $f;
