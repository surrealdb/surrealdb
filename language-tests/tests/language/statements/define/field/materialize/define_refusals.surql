/**
[env]
namespace = true
database = true

[test]
reason = """
	DEFINE FIELD refuses a MATERIALIZE INTO clause whose destination cannot hold
	the list the clause keeps: a table the database does not hold, a destination
	the table does not declare, one that is not READONLY, one of the wrong type or
	with a length limit, a COMPUTED one, the id, the field the clause is defined
	on, a destination on a table other than the one the field references, or one
	another clause fills already. The same goes for a condition that writes. A
	refused definition leaves no trace.
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
error = "An error occurred: MATERIALIZE INTO nowhere.posts requires table `nowhere` to exist"

[[test.results]]
error = "An error occurred: MATERIALIZE INTO author.missing requires field `missing` to be defined on table `author` first"

[[test.results]]
error = "An error occurred: MATERIALIZE INTO author.plain requires field `plain` to be READONLY: the engine keeps its list"

[[test.results]]
error = "An error occurred: MATERIALIZE INTO author.wrong requires field `wrong` to hold an array of record<post> with no length limit"

[[test.results]]
error = "An error occurred: MATERIALIZE INTO author.limited requires field `limited` to hold an array of record<post> with no length limit"

[[test.results]]
error = "An error occurred: MATERIALIZE INTO author.derived cannot fill a COMPUTED field"

[[test.results]]
error = "An error occurred: MATERIALIZE INTO other.other_ok requires field `a` to reference records of table `other` only"

[[test.results]]
error = "An error occurred: MATERIALIZE INTO author.ok requires field `a` to reference records of table `author` only"

[[test.results]]
error = "An error occurred: MATERIALIZE INTO author.ok requires field `a` to reference records of table `author` only"

[[test.results]]
error = "An error occurred: MATERIALIZE INTO author.me cannot fill the field it is defined on"

[[test.results]]
error = "An error occurred: MATERIALIZE INTO author.id requires the destination to be a top-level field other than `id`"

[[test.results]]
error = "The MATERIALIZE INTO clause must not have effects: `CREATE` writes data"

[[test.results]]
value = "NONE"

[[test.results]]
error = "An error occurred: MATERIALIZE INTO author.ok is filled by field `a` of table `post` already"

[[test.results]]
value = "{ events: {  }, fields: { a: 'DEFINE FIELD a ON post TYPE record<author> REFERENCE ON DELETE IGNORE MATERIALIZE INTO author.ok PERMISSIONS FULL' }, indexes: {  }, lives: {  }, migrations: { generation: 2, guaranteed: [], migrations: [] }, tables: {  } }"

[[test.results]]
value = """{ events: {  }, fields: { derived: 'DEFINE FIELD derived ON author COMPUTED [] PERMISSIONS FULL', limited: 'DEFINE FIELD limited ON author TYPE array<record<post>, 3> READONLY PERMISSIONS FULL', "limited.*": 'DEFINE FIELD limited.* ON author TYPE record<post> PERMISSIONS FULL', me: 'DEFINE FIELD me ON author TYPE array<record<author>> DEFAULT [] READONLY PERMISSIONS FULL', "me.*": 'DEFINE FIELD me.* ON author TYPE record<author> PERMISSIONS FULL', ok: 'DEFINE FIELD ok ON author TYPE array<record<post>> DEFAULT [] READONLY PERMISSIONS FULL', "ok.*": 'DEFINE FIELD ok.* ON author TYPE record<post> PERMISSIONS FULL', plain: 'DEFINE FIELD plain ON author TYPE array<record<post>> DEFAULT [] PERMISSIONS FULL', "plain.*": 'DEFINE FIELD plain.* ON author TYPE record<post> PERMISSIONS FULL', wrong: 'DEFINE FIELD wrong ON author TYPE array<string> DEFAULT [] READONLY PERMISSIONS FULL', "wrong.*": 'DEFINE FIELD wrong.* ON author TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, migrations: { generation: 6, guaranteed: [], migrations: [] }, tables: {  } }"""
*/

DEFINE TABLE author SCHEMAFULL;
DEFINE TABLE other SCHEMAFULL;
DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD plain ON author TYPE array<record<post>> DEFAULT [];
DEFINE FIELD wrong ON author TYPE array<string> DEFAULT [] READONLY;
DEFINE FIELD limited ON author TYPE array<record<post>, 3> READONLY;
DEFINE FIELD derived ON author COMPUTED [];
DEFINE FIELD ok ON author TYPE array<record<post>> DEFAULT [] READONLY;
DEFINE FIELD me ON author TYPE array<record<author>> DEFAULT [] READONLY;
DEFINE FIELD other_ok ON other TYPE array<record<post>> DEFAULT [] READONLY;
-- the table is not defined
DEFINE FIELD a ON post TYPE record<nowhere> REFERENCE MATERIALIZE INTO nowhere.posts;
-- the field is not declared
DEFINE FIELD a ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.missing;
-- the destination is not READONLY
DEFINE FIELD a ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.plain;
-- the destination holds another type
DEFINE FIELD a ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.wrong;
-- the destination limits its length
DEFINE FIELD a ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.limited;
-- the destination is computed
DEFINE FIELD a ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.derived;
-- the field references another table than the destination's
DEFINE FIELD a ON post TYPE record<author> REFERENCE MATERIALIZE INTO other.other_ok;
-- the field does not name one table
DEFINE FIELD a ON post TYPE record<author | other> REFERENCE MATERIALIZE INTO author.ok;
DEFINE FIELD a ON post TYPE option<record> REFERENCE MATERIALIZE INTO author.ok;
-- the destination is the field itself
DEFINE FIELD OVERWRITE me ON author TYPE array<record<author>> READONLY REFERENCE MATERIALIZE INTO author.me;
-- the id is no destination
DEFINE FIELD a ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.id;
-- a condition that writes
DEFINE FIELD a ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.ok WHERE (CREATE other:1);
-- an accepted definition, then a second clause into the same destination
DEFINE FIELD a ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.ok;
DEFINE FIELD b ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.ok;
INFO FOR TABLE post;
INFO FOR TABLE author;
