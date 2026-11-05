/**
[env]
imports = ["reproductions/a_guaranteed_record_and_a_write_only_caller_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }

[test]
reason = """
A conditional `UPSERT` writes its guaranteed record when it matched nothing,
and a record the statement matched stays matched whether or not the caller is
shown it. A caller who may write a table and not select it matches records
there without seeing them: the `PERMISSIONS FOR select` refusal that withholds
each row lands after the record is stored, so the statement matched every
record it updated and owes no invented one.

The event is the test's only window on the table: the caller may select nothing
there, so what was written is read back through the records the event wrote on
a table the caller may read.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "['UPDATE']"
*/

-- Matches `hidden_writable:1` and updates it. The caller may not select the
-- record, so no row is reported.
UPSERT hidden_writable SET n = 1 WHERE n = 0 RETURN VALUE n;

-- One update, and nothing invented on top of it.
SELECT VALUE did FROM trace ORDER BY did;
