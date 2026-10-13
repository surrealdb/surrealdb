/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A recursion path whose body repeats the recursion with `.@` runs once its loop is lowered to the physical dialect, and a `.@` outside any recursion raises the user error"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ tree: { id: t:1, next: [{ id: t:2, next: [{ id: t:3, next: [] }] }] } }]"

[[test.results]]
value = "[{ id: t:1, next: [{ id: t:2, next: [{ id: t:3, next: [] }] }] }, { id: t:2, next: [{ id: t:3, next: [] }] }, { id: t:3, next: [] }]"

[[test.results]]
value = "{ id: t:1, next: [{ id: t:2, next: [{ id: t:3, next: [] }] }] }"

[[test.results]]
error = "Tried to use a `@` repeat recurse symbol in a position where it is not supported"

[[test.results]]
value = "'not reached'"

[[test.results]]
error = "Tried to use a `@` repeat recurse symbol in a position where it is not supported"
*/
CREATE t:1, t:2, t:3 RETURN NONE;
RELATE t:1->e:1->t:2 RETURN NONE;
RELATE t:2->e:2->t:3 RETURN NONE;
SELECT @.{1..3}.{ id, next: ->e->t.@ } AS tree FROM t:1;
SELECT VALUE @.{1..3}.{ id, next: ->e->t.@ } FROM t ORDER BY id;
t:1.{1..3}.{ id, next: ->e->t.@ };
t:1.@.{1..3}.{ id, next: ->e->t.@ };
IF false { t:1.@ } ELSE { 'not reached' };
SELECT VALUE id.@ FROM t:1;
