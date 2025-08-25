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
value = "{ events: {  }, fields: { primary: 'DEFINE FIELD primary ON product TYPE number VALUE 123.456f PERMISSIONS FULL', quaternary: 'DEFINE FIELD quaternary ON product TYPE bool VALUE array::all([1, 2]) PERMISSIONS FULL', secondary: 'DEFINE FIELD secondary ON product TYPE bool DEFAULT true VALUE $value PERMISSIONS FULL', tertiary: \"DEFINE FIELD tertiary ON product TYPE string DEFAULT 'hello' VALUE 'tester' PERMISSIONS FULL\" }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ events: [], fields: [ { flex: false, kind: 'number', name: 'primary', permissions: { create: true, select: true, update: true }, readonly: false, value: '123.456f', what: 'product' }, { flex: false, kind: 'bool', name: 'quaternary', permissions: { create: true, select: true, update: true }, readonly: false, value: 'array::all([1, 2])', what: 'product' }, { default: 'true', default_always: false, flex: false, kind: 'bool', name: 'secondary', permissions: { create: true, select: true, update: true }, readonly: false, value: '$value', what: 'product' }, { default: \"'hello'\", default_always: false, flex: false, kind: 'string', name: 'tertiary', permissions: { create: true, select: true, update: true }, readonly: false, value: \"'tester'\", what: 'product' } ], indexes: [ ], lives: [ ], tables: [ ] }"

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
INFO FOR TABLE product;
INFO FOR TABLE product STRUCTURE;
--
CREATE product:test SET primary = NULL;
CREATE product:test SET secondary = 'oops';
CREATE product:test SET tertiary = 123;
CREATE product:test;
--
UPSERT product:test SET primary = 654.321;
UPSERT product:test SET secondary = false;
UPSERT product:test SET tertiary = 'something';
