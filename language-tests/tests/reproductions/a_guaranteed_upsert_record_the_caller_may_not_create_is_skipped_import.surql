/**
[env]
namespace = true
database = true
auth = { level = "owner" }
[test]
reason = "Setup for a_guaranteed_upsert_record_the_caller_may_not_create_is_skipped.surql."

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
*/
DEFINE TABLE c_none SCHEMALESS PERMISSIONS FOR select, update FULL, FOR create NONE;
DEFINE TABLE u_none SCHEMALESS PERMISSIONS FOR select, create FULL, FOR update NONE;
DEFINE TABLE c_pred SCHEMALESS PERMISSIONS FOR select, update FULL, FOR create WHERE x = 1;
DEFINE TABLE all_full SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE sel_none SCHEMALESS PERMISSIONS FOR select NONE, FOR create, update FULL;
