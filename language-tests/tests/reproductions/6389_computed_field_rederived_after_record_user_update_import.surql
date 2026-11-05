/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Setup for 6389_computed_field_rederived_after_record_user_update.surql: a table every principal may write, with a COMPUTED field that reads another field of the same record."
run = false
*/

DEFINE TABLE proxy SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD run ON proxy TYPE bool DEFAULT false;
DEFINE FIELD result ON proxy COMPUTED IF run { 1 } ELSE { NONE };
