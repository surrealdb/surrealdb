/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	The columns of an index over a table whose record id names no components are
	checked as they always were: on a schemaless table any path may be indexed,
	whatever it is spelled as, `id[0]` included, and on a SCHEMAFULL table a path
	no field covers is refused with the column as written, escapes and all,
	while `id[0]` under an `id` whose array type names nothing passes as a path
	beneath a field whose type admits one.
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
value = "{ events: {  }, fields: {  }, indexes: { a: 'DEFINE INDEX a ON loose FIELDS `weird-name`', b: 'DEFINE INDEX b ON loose FIELDS tags.*', c: 'DEFINE INDEX c ON loose FIELDS id[0]', d: 'DEFINE INDEX d ON loose FIELDS a.b.c, nested[0].x', e: 'DEFINE INDEX e ON loose FIELDS `select`, `ünï`' }, lives: {  }, tables: {  } }"

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
value = "NONE"

[[test.results]]
error = "The field '`weird-name`' does not exist"

[[test.results]]
error = "The field 'nothing[0]' does not exist"

[[test.results]]
error = "The field 'id[0]' does not exist"

[[test.results]]
error = "The field 'select' does not exist"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"
*/

-- Schemaless: every spelling of a path no field defines is accepted.
DEFINE INDEX a ON loose FIELDS `weird-name`;
DEFINE INDEX b ON loose FIELDS tags[*];
DEFINE INDEX c ON loose FIELDS id[0];
DEFINE INDEX d ON loose FIELDS a.b.c, nested[0].x;
DEFINE INDEX e ON loose FIELDS `select`, `ünï`;
INFO FOR TABLE loose;

-- An array id that names nothing.
DEFINE TABLE pair SCHEMAFULL;
DEFINE FIELD id ON pair TYPE [string, int];
DEFINE FIELD v ON pair TYPE int;
DEFINE INDEX f ON pair FIELDS id[0];
DEFINE INDEX g ON pair FIELDS id[1], v;

-- SCHEMAFULL: a path no field covers is refused as written.
DEFINE TABLE strict SCHEMAFULL;
DEFINE INDEX h ON strict FIELDS `weird-name`;
DEFINE INDEX i ON strict FIELDS nothing[0];
DEFINE INDEX j ON strict FIELDS id[0];
DEFINE INDEX k ON strict FIELDS `select`;
INFO FOR TABLE strict;
