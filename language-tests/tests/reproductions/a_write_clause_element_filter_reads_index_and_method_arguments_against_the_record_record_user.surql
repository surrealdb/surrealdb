/**
[env]
imports = ["reproductions/a_write_clause_element_filter_reads_index_and_method_arguments_against_the_record_record_user_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "A record user's update judged by an update permission, and its `WHERE`, holding an element filter whose body reads the record through an index argument or a method argument, runs: the filter reads the record. A closure in a write's `WHERE` is declined, so only the filter written without a closure is held."

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[pu_cl_m:2]"

[[test.results]]
value = "[pu_cf_m:2]"

[[test.results]]
value = "[uw_cf_m:2]"
*/

UPDATE pu_cl_i SET q = 1 RETURN VALUE id;
UPDATE pu_cf_i SET q = 1 RETURN VALUE id;
UPDATE uw_cf_i SET q = 1 WHERE [3][WHERE $this >= [0, 8, 100][idx]] != [] RETURN VALUE id;
UPDATE pu_cl_m SET q = 1 RETURN VALUE id;
UPDATE pu_cf_m SET q = 1 RETURN VALUE id;
UPDATE uw_cf_m SET q = 1 WHERE [3][WHERE $this >= [0].concat([lim]).max()] != [] RETURN VALUE id;
