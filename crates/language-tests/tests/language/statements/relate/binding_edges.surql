/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: knows:one, in: person:john, out: person:jane }]"

[[test.results]]
value = "[{ id: knows:two, in: person:john, out: person:jane }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: knows:three, in: person:john, out: person:jane }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: knows:four, in: person:john, out: person:jane }]"

*/

-- Test that $in -> edge_name -> $out syntax works
LET $john = person:john;
LET $jane = person:jane;
RELATE $john -> knows -> $jane SET id = knows:one;

RELATE $john -> knows:two -> $jane;

-- Test that $in -> $kind -> $out syntax works
LET $kind = type::table("knows");
RELATE $john -> $kind -> $jane SET id = knows:three;

LET $kind = <record> "knows:four";
RELATE $john -> $kind -> $jane;
