/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	ALTER TABLE and ALTER FIELD over tables whose record id names no components
	behave as they always have: a relation is altered in place, a plain table
	becomes a relation, and the `id` field is retyped, commented and untyped,
	each definition reading back as written.
"""

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
value = "{ events: {  }, fields: { id: 'DEFINE FIELD id ON rel TYPE string PERMISSIONS FULL', in: 'DEFINE FIELD in ON rel TYPE record<a> PERMISSIONS FULL', out: 'DEFINE FIELD out ON rel TYPE record<c> PERMISSIONS FULL' }, indexes: {  }, lives: {  }, migrations: { generation: 2, guaranteed: [], migrations: [] }, tables: {  } }"

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
value = """{ events: {  }, fields: { id: "DEFINE FIELD id ON plain TYPE int COMMENT 'the id' PERMISSIONS FULL", in: 'DEFINE FIELD in ON plain TYPE record PERMISSIONS FULL', out: 'DEFINE FIELD out ON plain TYPE record PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: {  }, fields: { id: "DEFINE FIELD id ON plain COMMENT 'the id' PERMISSIONS FULL", in: 'DEFINE FIELD in ON plain TYPE record PERMISSIONS FULL', out: 'DEFINE FIELD out ON plain TYPE record PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"""
*/

DEFINE TABLE rel TYPE RELATION IN a OUT b;
ALTER TABLE rel COMMENT 'edges';
ALTER TABLE rel TYPE RELATION IN a OUT c;
DEFINE FIELD id ON rel TYPE string;
ALTER TABLE rel SCHEMAFULL;
INFO FOR TABLE rel;

DEFINE FIELD id ON plain TYPE [string, int];
ALTER TABLE plain TYPE RELATION;
ALTER TABLE plain TYPE NORMAL;
ALTER FIELD id ON plain COMMENT 'the id';
ALTER FIELD id ON plain TYPE int;
INFO FOR TABLE plain;
ALTER FIELD id ON plain DROP TYPE;
INFO FOR TABLE plain;
