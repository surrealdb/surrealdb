/**
[test]
reason = "Verify that RELATE statements work correctly with various edge type bindings"

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

-- Test basic RELATE with literal edge name and explicit ID
LET $john = person:john;
LET $jane = person:jane;
RELATE $john -> knows -> $jane SET id = knows:one;

-- Test RELATE with literal edge record ID (no SET clause)
RELATE $john -> knows:two -> $jane;

-- Test RELATE with $kind binding using type::table() function
LET $kind = type::table("knows");
RELATE $john -> $kind -> $jane SET id = knows:three;

-- Test RELATE with $kind binding using explicit record ID
LET $kind = <record> "knows:four";
RELATE $john -> $kind -> $jane;
