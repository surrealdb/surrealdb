/**
[env]
namespace = true
database = true

[test]
reason = "An object-literal field of a named type that is absent is judged as NONE: it may be absent only where the type admits NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Could not cast into `{ a: pos_int }`: Expected `pos_int (int)` but found `NONE`"

[[test.results]]
value = "{ a: 2 }"

[[test.results]]
value = "{  }"

[[test.results]]
value = "{  }"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `o` of `t:1`: Expected `pos_int (int)` but found `NONE`"

[[test.results]]
value = "[{ id: t:2, o: { a: 1 } }]"
*/

DEFINE TYPE pos_int AS int ASSERT $value > 0;
DEFINE TYPE maybe_int AS option<int>;
RETURN <{ a: pos_int }> {};
RETURN <{ a: pos_int }> { a: '2' };
RETURN <{ a: option<pos_int> }> {};
RETURN <{ a: maybe_int }> {};
DEFINE FIELD o ON t TYPE { a: pos_int };
CREATE t:1 SET o = {};
CREATE t:2 SET o = { a: 1 };
