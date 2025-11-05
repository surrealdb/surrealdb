/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: { }, fields: { primary: 'DEFINE FIELD primary ON product TYPE number DEFAULT ALWAYS 123.456f PERMISSIONS FULL' }, indexes: { }, lives: { }, tables: { } }"

[[test.results]]
value = "{ events: [ ], fields: [ { default: '123.456f', default_always: true, flex: false, kind: 'number', name: 'primary', permissions: { create: true, select: true, update: true }, readonly: false, what: 'product' } ], indexes: [ ], lives: [ ], tables: [ ] }"

[[test.results]]
error = "Couldn't coerce value for field `primary` of `product:test`: Expected `number` but found `NULL`"


[[test.results]]
value = "[{ id: product:test, primary: 123.456f }]"

[[test.results]]
value = "[{ id: product:test, primary: 654.321f }]"

[[test.results]]
value = "[{ id: product:test, primary: 123.456f }]"

[[test.results]]
value = "[{ id: product:test, primary: 123.456f }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: post:test, tags: [] }]"

[[test.results]]
value = "[{ id: post:test, tags: [{ color: 'red', name: 'test' }] }]"

[[test.results]]
value = "[{ id: post:test, tags: [{ color: 'red', name: 'test' }, { color: 'blue', name: 'test' }] }]"

*/

DEFINE TABLE product SCHEMAFULL;
DEFINE FIELD primary ON product TYPE number DEFAULT ALWAYS 123.456;
--
INFO FOR TABLE product;
INFO FOR TABLE product STRUCTURE;
--
CREATE product:test SET primary = NULL;
--
CREATE product:test;
UPSERT product:test SET primary = 654.321;
UPSERT product:test SET primary = NONE;
UPSERT product:test CONTENT { };
--
DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD tags ON post TYPE array<object> DEFAULT ALWAYS [ ];
DEFINE FIELD tags.*.color ON post TYPE string DEFAULT ALWAYS 'red';
DEFINE FIELD tags.*.name ON post TYPE string;
--
CREATE post:test;
UPSERT post:test SET tags += { name: 'test' };
UPSERT post:test SET tags += { name: 'test', color: 'blue' };