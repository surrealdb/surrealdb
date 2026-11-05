/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A recursion whose body holds a subquery that reads no row, or a filter on a name a block binds, steps as the operator tree steps"

[[test.results]]
value = "[{ id: person:1 }, { id: person:2 }, { id: person:3 }]"

[[test.results]]
value = "[{ id: knows:1, in: person:1, out: person:2 }]"

[[test.results]]
value = "[{ id: knows:2, in: person:2, out: person:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[person:3]"

[[test.results]]
value = "[person:3]"

[[test.results]]
value = "[person:2]"
*/
CREATE person:1, person:2, person:3;
RELATE person:1->knows:1->person:2;
RELATE person:2->knows:2->person:3;
LET $me = person:1;
SELECT VALUE id FROM $me.{..3}(->knows->(person WHERE id != $me));
SELECT VALUE id FROM person:1.{..3}(->knows->(person WHERE id IN (SELECT VALUE id FROM person)));
{ LET $x = person:3; RETURN person:1.{..3+collect}(->knows->(person WHERE id != $x)); };
