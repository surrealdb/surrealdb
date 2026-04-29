/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Relation tables auto-generate in/out fields; re-defining with OVERWRITE (as export now produces) must not error"
issue = 6075

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { in: 'DEFINE FIELD in ON myRel TYPE record<users> PERMISSIONS FULL', out: 'DEFINE FIELD out ON myRel TYPE record<stuff> PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"
*/

-- Define a relation table which auto-generates in/out fields
DEFINE TABLE myRel TYPE RELATION IN users OUT stuff SCHEMALESS PERMISSIONS NONE;

-- Simulate what the fixed export produces: DEFINE FIELD OVERWRITE for the auto-generated fields
DEFINE FIELD OVERWRITE in ON myRel TYPE record<users> PERMISSIONS FULL;
DEFINE FIELD OVERWRITE out ON myRel TYPE record<stuff> PERMISSIONS FULL;

INFO FOR TABLE myRel;
