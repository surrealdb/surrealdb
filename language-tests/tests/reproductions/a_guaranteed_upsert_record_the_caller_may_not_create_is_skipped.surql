/**
[env]
imports = ["reproductions/a_guaranteed_upsert_record_the_caller_may_not_create_is_skipped_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "accounts:1" }

[test]

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1]"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
value = "[]"

*/
UPSERT c_none SET x = THROW 'boom' WHERE true;
UPSERT u_none SET x = THROW 'boom' WHERE true;
UPSERT c_pred SET x = THROW 'boom' WHERE true;
UPSERT c_pred SET x = 2 WHERE true;
UPSERT c_pred SET x = 1 WHERE true RETURN VALUE x;
UPSERT all_full SET x = THROW 'boom' WHERE true;
UPSERT sel_none SET x = THROW 'boom' WHERE true;
UPSERT sel_none SET x = 1 WHERE true;
