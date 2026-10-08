/**
[env]
namespace = true
database = true

[test]
reason = "ALTER FIELD checks the definition it forms as DEFINE FIELD does: FLEXIBLE needs a kind containing an object and a SCHEMAFULL table, judged on the merged definition, so a later ALTER of a FLEXIBLE field's kind is checked too. A refused ALTER leaves the field as it was."

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
error = "An error occurred: FLEXIBLE can only be used with types containing object"

[[test.results]]
error = "An error occurred: FLEXIBLE can only be used with types containing object"

[[test.results]]
error = "An error occurred: FLEXIBLE can only be used in SCHEMAFULL tables"

[[test.results]]
value = "NONE"

[[test.results]]
error = "An error occurred: FLEXIBLE can only be used with types containing object"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { doc: 'DEFINE FIELD doc ON loose TYPE object PERMISSIONS FULL', name: 'DEFINE FIELD name ON loose TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ events: {  }, fields: { doc: 'DEFINE FIELD doc ON strict TYPE string PERMISSIONS FULL', name: 'DEFINE FIELD name ON strict TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"
*/

DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD name ON loose TYPE string;
DEFINE FIELD doc ON loose TYPE object;
DEFINE TABLE strict SCHEMAFULL;
DEFINE FIELD name ON strict TYPE string;
DEFINE FIELD doc ON strict TYPE object;

-- Refused: the kind holds no object.
ALTER FIELD name ON loose FLEXIBLE;
ALTER FIELD name ON strict FLEXIBLE;
-- Refused: the table is not SCHEMAFULL.
ALTER FIELD doc ON loose FLEXIBLE;
-- Admitted.
ALTER FIELD doc ON strict FLEXIBLE;
-- Refused: the merged definition stays FLEXIBLE, and its new kind holds no
-- object.
ALTER FIELD doc ON strict TYPE string;
-- Admitted: FLEXIBLE is dropped with the kind that needed it.
ALTER FIELD doc ON strict DROP FLEXIBLE TYPE string;

INFO FOR TABLE loose;
INFO FOR TABLE strict;
