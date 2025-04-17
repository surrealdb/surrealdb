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
error = "Couldn't coerce value for field `primary` of `product:test`: Expected `number` but found `NULL`"

[[test.results]]
error = "Couldn't coerce value for field `secondary` of `product:test`: Expected `bool` but found `'oops'`"

[[test.results]]
error = "Couldn't coerce value for field `tertiary` of `product:test`: Expected `string` but found `123`"

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
