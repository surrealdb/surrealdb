/**
[env]
imports = ["upgrade/define/3.00.00_table_view_reads_itself_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
# Only the current checkout maintains the views over a view's rows, which is
# what brings `s` back to itself: every released reader writes `s:2` once and
# stops.
version = ">=3.4.0-a"
reason = """
On the current checkout, a write that reaches a view reading its own table, as
an older binary stored it, fails with an error naming the view instead of
recursing until the server aborts. The failed write is undone, the server goes
on answering, and the view keeps the rows the older binary maintained.
"""

[[test.results]]
value = "['s']"

[[test.results]]
error = "Cannot maintain the view `s`: it reads itself"

[[test.results]]
value = "[{ id: src:1, n: 1 }]"

[[test.results]]
value = "[{ id: s:1, n: 2 }]"

*/
-- `s` is registered as a view reading its own table.
object::keys((INFO FOR TABLE s).tables);
-- A write to the source reaches `s`, whose row reaches `s` again.
CREATE src:2 SET n = 2 RETURN NONE;
-- The write left nothing behind, and the server still answers.
SELECT * FROM src ORDER BY id;
SELECT * FROM s ORDER BY id;
