/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
No write on this build reaches a view whose sources include itself, because the
definition is refused (a_view_cannot_read_itself.surql): neither the definition
nor rows its sources already hold build such a view, the refused definition
leaves no table behind, and the datastore goes on serving statements. The view
defined again over tables that do not read it is accepted and maintained.

A store an older release wrote can still hold a view that reads itself. A write
that reaches one fails with an error naming the view and is undone
(upgrade/define/3.00.00_table_view_reads_itself_current.surql, and
`exec::vm::tests_view_reentry`, which writes such a catalog directly).
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: Cannot define the view `s` over itself"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: src:1, n: 1 }]"

[[test.results]]
value = "['src']"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid query: Cannot define the view `t` over itself"

[[test.results]]
value = "['other', 'src']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: s:1, n: 1 }, { id: s:2, n: 2 }]"
*/

DEFINE TABLE src SCHEMALESS;
DEFINE TABLE s AS SELECT n + 1 AS n FROM src, s;
CREATE src:1 SET n = 1 RETURN NONE;
SELECT * FROM src;
object::keys((INFO FOR DB).tables);

CREATE other:1 SET n = 1 RETURN NONE;
DEFINE TABLE t AS SELECT n + 1 AS n FROM other, t;
object::keys((INFO FOR DB).tables);

DEFINE TABLE OVERWRITE s AS SELECT n FROM src;
CREATE src:2 SET n = 2 RETURN NONE;
SELECT * FROM s ORDER BY id;
