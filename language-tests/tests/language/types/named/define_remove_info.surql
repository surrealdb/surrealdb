/**
[env]
namespace = true
database = true

[test]
reason = "DEFINE and REMOVE TYPE over a union, a kind and an object, and their listing in INFO FOR DB"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The type 'posint' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, types: { address: "DEFINE TYPE address AS { street: none | null | string, zip: none | null | int } SQL TYPE POSTGRESQL '(street text, zip integer)' PERMISSIONS FULL", feeling: "DEFINE TYPE feeling AS mood | none SQL TYPE POSTGRESQL 'mood' PERMISSIONS FULL", mood: "DEFINE TYPE mood AS 'sad' | 'ok' PERMISSIONS FULL", posint: "DEFINE TYPE posint AS int CONSTRAINT positive ASSERT $value > 0 ASSERT $value < 1000 DEFAULT 1 SQL TYPE POSTGRESQL 'integer' PERMISSIONS FULL" }, users: {  } }"""

[[test.results]]
value = """{ accesses: [], analyzers: [], apis: [], buckets: [], compatibility: 4, configs: [], extensions: [], functions: [], models: [], params: [], sequences: [], tables: [], types: [{ asserts: [], kind: '{ street: none | null | string, zip: none | null | int }', name: 'address', permissions: true, sql_type: '(street text, zip integer)' }, { asserts: [], kind: 'mood | none', name: 'feeling', permissions: true, sql_type: 'mood' }, { asserts: [], kind: "'sad' | 'ok'", labels: ['sad', 'ok'], name: 'mood', permissions: true }, { asserts: [{ check: '$value > 0', name: 'positive' }, { check: '$value < 1000' }], default: '1', kind: 'int', name: 'posint', permissions: true, sql_type: 'integer' }], users: [] }"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "The type 'nope' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, types: { feeling: "DEFINE TYPE feeling AS string SQL TYPE POSTGRESQL 'text' PERMISSIONS FULL", mood: "DEFINE TYPE mood AS 'sad' | 'ok' PERMISSIONS FULL" }, users: {  } }"""

*/

DEFINE TYPE mood AS 'sad' | 'ok';
DEFINE TYPE posint AS int CONSTRAINT positive ASSERT $value > 0 ASSERT $value < 1000 DEFAULT 1 SQL TYPE POSTGRESQL 'integer';
DEFINE TYPE feeling AS mood | none SQL TYPE POSTGRESQL 'mood';
DEFINE TYPE address AS { street: none | null | string, zip: none | null | int } SQL TYPE POSTGRESQL '(street text, zip integer)';
DEFINE TYPE posint AS int;
DEFINE TYPE IF NOT EXISTS posint AS int;
INFO FOR DB;
INFO FOR DB STRUCTURE;
DEFINE TYPE OVERWRITE feeling AS string SQL TYPE POSTGRESQL 'text';
REMOVE TYPE nope;
REMOVE TYPE IF EXISTS nope;
REMOVE TYPE posint;
REMOVE TYPE address;
REMOVE TYPE IF EXISTS address;
INFO FOR DB;
