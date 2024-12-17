/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found NULL for field `primary`, with record `product:test`, but expected a number"

[[test.results]]
error = "Found 'oops' for field `secondary`, with record `product:test`, but expected a bool"

[[test.results]]
error = "Found 123 for field `tertiary`, with record `product:test`, but expected a string"

[[test.results]]
value = "[{ id: product:test, primary: 123.456f, quaternary: true, secondary: true, tertiary: 'tester' }]"

[[test.results]]
value = "[{ id: product:test, primary: 123.456f, quaternary: true, secondary: true, tertiary: 'tester' }]"

[[test.results]]
value = "[{ id: product:test, primary: 123.456f, quaternary: true, secondary: false, tertiary: 'tester' }]"

[[test.results]]
value = "[{ id: product:test, primary: 123.456f, quaternary: true, secondary: false, tertiary: 'tester' }]"

*/

DEFINE TABLE product SCHEMAFULL;
DEFINE FIELD primary ON product TYPE number VALUE 123.456;
DEFINE FIELD secondary ON product TYPE bool DEFAULT true VALUE $value;
DEFINE FIELD tertiary ON product TYPE string DEFAULT 'hello' VALUE 'tester';
DEFINE FIELD quaternary ON product TYPE bool VALUE array::all([1, 2]);
--
CREATE product:test SET primary = NULL;
CREATE product:test SET secondary = 'oops';
CREATE product:test SET tertiary = 123;
CREATE product:test;
--
UPSERT product:test SET primary = 654.321;
UPSERT product:test SET secondary = false;
UPSERT product:test SET tertiary = 'something';
