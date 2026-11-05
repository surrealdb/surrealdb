/**
[env]
imports = ["language/closure/a_closure_passing_the_row_to_a_function_reads_the_row_where_it_is_written_record_user_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = """
A table permission whose predicate holds a closure that passes the row to a user-defined function
admits the rows the function's value for that row admits: the call is evaluated over the row the
literal is written over, so only `pt:2` (threshold 0) is admitted; `pt:1` (threshold 100) and `pt:3`
(threshold 8) hold no element that reaches theirs.
"""

[[test.results]]
value = "[{ id: pt:2 }]"
*/
SELECT id FROM pt ORDER BY id;
