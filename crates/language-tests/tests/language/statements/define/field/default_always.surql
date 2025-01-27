/**
[test]

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"

[[test.results]]
error = "Found NULL for field `primary`, with record `product:test`, but expected a number"

[[test.results]]
value = "[{ id: product:test, primary: 123.456f }]"
[[test.results]]
value = "[{ id: product:test, primary: 654.321f }]"
[[test.results]]
value = "[{ id: product:test, primary: 123.456f }]"
[[test.results]]
value = "[{ id: product:test, primary: 123.456f }]"

*/

DEFINE TABLE product SCHEMAFULL;
DEFINE FIELD primary ON product TYPE number DEFAULT ALWAYS 123.456;
--
CREATE product:test SET primary = NULL;
--
CREATE product:test;
UPSERT product:test SET primary = 654.321;
UPSERT product:test SET primary = NONE;
UPSERT product:test CONTENT {};
