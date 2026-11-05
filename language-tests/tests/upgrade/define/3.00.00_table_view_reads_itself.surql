/**
[env]
imports = ["upgrade/define/3.00.00_table_view_reads_itself_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
reason = """
A view that reads its own table, as every released binary stores it, keeps the
rows the older binary maintained, and a write to its source reaches it.

The current checkout is the exception and says so through `[[test.repair]]`: it
maintains the views over a view's rows, which brings each row `s` writes back
to `s`, so it refuses the write until the view is defined again.
3.00.00_table_view_reads_itself_current.surql pins what the refusal says.

The body is idempotent, since a repair runs it twice: the write is an `UPSERT`,
and before the repair it fails and writes nothing.
"""

[[test.repair]]
since = ">=3.4.0-a"
script = "upgrade/define/3.00.00_table_view_reads_itself_redefine.surql"
unrepaired = "broken"
reason = """
The current checkout maintains the views over a view's rows, so a view whose
sources include its own table reaches itself again with every row it writes. A
write to any table such a view reads fails, naming the view, until the view is
defined again over tables that do not read it, or removed. Released binaries
never maintained a view's own views, so a store they wrote can hold one."""

[[test.results]]
value = "[{ id: s:1, n: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: s:1, n: 2 }, { id: s:2, n: 3 }]"

*/
-- The row the older binary maintained.
SELECT * FROM s ORDER BY id;
-- A write to the source reaches the view.
UPSERT src:2 SET n = 2 RETURN NONE;
SELECT * FROM s ORDER BY id;
