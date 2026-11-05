/**
[env]
imports = ["reproductions/a_record_user_of_an_undefined_table_reads_as_none_through_auth_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:b" }

[test]
reason = """A record read that finds no record asks whether the table is defined, and a table a block defines implicitly, by a write, is defined for the reads that follow it in the block, even when an earlier read of the block saw it undefined. A record id naming a table nothing in the block defines still fails with `TbNotFound`."""

[[test.results]]
value = "[]"

[[test.results]]
error = "The table 'absent' does not exist"

*/

{ CREATE fresh:1 SET n = 1; UPDATE fresh SET n += 1; DELETE fresh:1; RETURN SELECT * FROM fresh };
{ RETURN SELECT * FROM absent:1 };
