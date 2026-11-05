/**
[env]
imports = ["language/closure/matrix/perm/sel_subq_cl_param_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "Register #28, row PermissionOrAssertFrame, admitted control: a table permission that reads another table is admitted. A copy of language/closure/matrix/PERM_select_subq_cl_param.surql, whose import defines the table."

[[test.results]]
value = "[]"
*/
SELECT VALUE n FROM pt_subq_cl_param ORDER BY n;
