/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: knows:foo, in: person:tobie, out: person:jaime }]"

[[test.results]]
value = "[{ id: knows:bar, in: person:tobie, out: person:jaime }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: knows:foo, in: person:tobie, out: person:jaime }]"

[[test.results]]
value = "[{ id: knows:bar, in: person:tobie, out: person:jaime }]"


*/

LET $tobie = person:tobie;
LET $jaime = person:jaime;
LET $relation = type::table("knows");
RELATE $tobie->$relation->$jaime CONTENT { id: "foo" };
RELATE $tobie->(type::table("knows"))->$jaime CONTENT { id: "bar" };
LET $relation = type::thing("knows:foo");
RELATE $tobie->$relation->$jaime;
RELATE $tobie->(type::thing("knows:bar"))->$jaime;
